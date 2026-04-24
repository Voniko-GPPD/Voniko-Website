"""
AI Engine for battery detection using ONNX Runtime GPU
Enhanced with GPU acceleration for faster inference
Supports: CUDA, DirectML (Windows), and CPU fallback
"""
import cv2
import numpy as np
from pathlib import Path
from typing import Tuple, List, Optional
import base64
from datetime import datetime
import os
import time
import threading

# Try to import ONNX Runtime with GPU support
ONNX_AVAILABLE = False
ONNX_GPU_AVAILABLE = False
PREFERRED_PROVIDER = None
ort = None

try:
    import onnxruntime as ort
    ONNX_AVAILABLE = True
    providers = ort.get_available_providers()
    
    # Check available GPU providers (priority: CUDA > DirectML > CPU)
    if 'CUDAExecutionProvider' in providers:
        ONNX_GPU_AVAILABLE = True
        PREFERRED_PROVIDER = 'CUDAExecutionProvider'
        print(f"[OK] ONNX Runtime CUDA GPU available")
    elif 'DmlExecutionProvider' in providers:
        ONNX_GPU_AVAILABLE = True
        PREFERRED_PROVIDER = 'DmlExecutionProvider'
        print(f"[OK] ONNX Runtime DirectML GPU available")
    else:
        PREFERRED_PROVIDER = 'CPUExecutionProvider'
        print(f"ONNX Runtime available (CPU only). Providers: {providers}")
except ImportError:
    print("Warning: onnxruntime not installed.")

# Fallback to YOLO if ONNX not available
YOLO_AVAILABLE = False
if not ONNX_AVAILABLE:
    try:
        from ultralytics import YOLO
        YOLO_AVAILABLE = True
        print("Using YOLO fallback (slower)")
    except ImportError:
        print("Warning: Neither ONNX Runtime nor ultralytics available.")


class AIEngine:
    """Singleton AI Engine for battery detection with ONNX Runtime GPU acceleration"""
    _instance = None
    _model = None
    _session = None  # ONNX Runtime session
    _use_onnx = False
    _load_error = None  # Last model load error message (None when loaded successfully)
    _lock = threading.Lock()  # Guards model load/reload and inference
    
    # Preprocessing configuration
    TARGET_SIZE = 1280  # Updated to match retrained model at 1280px
    CLAHE_CLIP_LIMIT = 2.0
    CLAHE_TILE_SIZE = (8, 8)
    
    # ==================== ADAPTIVE DETECTION CONFIGURATION ====================
    STANDARD_CONFIDENCE = 0.25
    SAHI_ENABLED = True
    SAHI_SLICE_SIZE = 1280  # Match model input size for best accuracy
    BATCH_SIZE = 8
    
    # Adaptive confidence range (auto-adjusted based on image brightness)
    CONFIDENCE_MIN = 0.25  # For dark images (lowered to catch edge/dim batteries)
    CONFIDENCE_MAX = 0.45  # For bright images (lowered to reduce missed detections)
    
    # Adaptive overlap range (auto-adjusted based on image size)
    OVERLAP_MIN = 0.15  # For smaller images
    OVERLAP_MAX = 0.25  # For larger images
    
    # Density-based detection
    DENSITY_CHECK_ENABLED = True
    SAHI_MIN_COUNT = 80
    SAHI_MIN_IMAGE_SIZE = 2000
    SAHI_MERGE_IOU = 0.35  # Less aggressive merge - only merge when overlap is high
    
    # Skip PASS 1 for production (always use SAHI for large images)
    SKIP_PASS1_FOR_LARGE_TRAYS = True
    
    # Common parameters (relaxed for better edge detection)
    DEFAULT_CONFIDENCE = 0.25
    DEFAULT_IOU_THRESHOLD = 0.5
    MIN_AREA_RATIO = 0.0002   # Relaxed: allow smaller detections
    MAX_AREA_RATIO = 0.25     # Relaxed: allow larger detections
    MIN_ASPECT_RATIO = 0.3    # Relaxed: more shape tolerance
    MAX_ASPECT_RATIO = 3.0    # Relaxed: more shape tolerance
    CENTER_DISTANCE_RATIO = 0.85  # Per-pair max radius threshold (adjacent batteries are ~2R apart, so this is safe)
    MIN_RADIUS_RATIO = 0.50   # Relaxed: allow more size variation
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if self._session is None and self._model is None:
            with self.__class__._lock:
                if self._session is None and self._model is None:
                    self._load_model()

    def reload_model(self):
        """Force reload the model (useful for hot-reload without service restart)."""
        with self.__class__._lock:
            self.__class__._session = None
            self.__class__._model = None
            self.__class__._use_onnx = False
            self.__class__._load_error = None
            self._load_model()

    @property
    def is_model_loaded(self) -> bool:
        """Return True only when a real AI model (ONNX or YOLO) is loaded."""
        return self._session is not None or self._model is not None

    @property
    def load_error(self) -> Optional[str]:
        """Return the last model load error, or None if loaded successfully."""
        return self.__class__._load_error
    
    # ==================== ADAPTIVE IMAGE ANALYSIS ====================
    
    def analyze_image_quality(self, image: np.ndarray) -> dict:
        """Analyze image brightness and contrast for adaptive parameter tuning"""
        gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
        
        brightness = float(np.mean(gray))  # 0-255
        contrast = float(np.std(gray))     # higher = more contrast
        
        # Normalize to 0-1 range
        brightness_norm = brightness / 255.0
        contrast_norm = min(contrast / 80.0, 1.0)  # 80 std = high contrast
        
        quality = {
            "brightness": brightness,
            "contrast": contrast,
            "brightness_norm": brightness_norm,
            "contrast_norm": contrast_norm
        }
        print(f"[ADAPTIVE] Image quality: brightness={brightness:.0f}/255 ({brightness_norm:.2f}), contrast={contrast:.0f} ({contrast_norm:.2f})")
        return quality
    
    def get_adaptive_confidence(self, image: np.ndarray) -> float:
        """Auto-adjust confidence based on image brightness and contrast.
        Dark/low-contrast images -> lower confidence to catch more batteries.
        Bright/high-contrast images -> higher confidence to reduce false positives."""
        quality = self.analyze_image_quality(image)
        
        # Score: 0 (dark/low contrast) to 1 (bright/high contrast)
        score = (quality["brightness_norm"] * 0.6 + quality["contrast_norm"] * 0.4)
        
        # Map score to confidence range
        confidence = self.CONFIDENCE_MIN + score * (self.CONFIDENCE_MAX - self.CONFIDENCE_MIN)
        confidence = max(self.CONFIDENCE_MIN, min(self.CONFIDENCE_MAX, confidence))
        
        print(f"[ADAPTIVE] Auto confidence: {confidence:.3f} (score={score:.2f})")
        return confidence
    
    def get_adaptive_overlap(self, image_shape: Tuple[int, int]) -> float:
        """Auto-adjust SAHI overlap based on image dimensions.
        Larger images need more overlap to avoid missing batteries at slice boundaries."""
        h, w = image_shape[:2]
        max_dim = max(w, h)
        
        # Scale overlap: small image (2000px) -> OVERLAP_MIN, large image (5000px+) -> OVERLAP_MAX
        ratio = (max_dim - 2000) / 3000.0  # 0 at 2000px, 1 at 5000px
        ratio = max(0.0, min(1.0, ratio))
        
        overlap = self.OVERLAP_MIN + ratio * (self.OVERLAP_MAX - self.OVERLAP_MIN)
        
        print(f"[ADAPTIVE] Auto overlap: {overlap:.2f} for {w}x{h} image (max_dim={max_dim})")
        return overlap
    
    def _load_model(self):
        """Load ONNX model with GPU acceleration or fallback to YOLO"""
        # Allow model directory to be overridden via environment variable
        env_models_dir = os.getenv("COUNT_BATTERIES_MODELS_DIR")
        if env_models_dir:
            models_dir = Path(env_models_dir)
        else:
            # Default: look in models/ inside the service root (count-batteries-service/models)
            models_dir = Path(__file__).parent.parent / "models"
        onnx_path = models_dir / "best.onnx"
        pt_path = models_dir / "best.pt"
        load_errors = []
        
        # Try ONNX first (faster)
        if ONNX_AVAILABLE and onnx_path.exists():
            try:
                # Configure ONNX Runtime session based on available provider
                if PREFERRED_PROVIDER == 'CUDAExecutionProvider':
                    providers = [
                        ('CUDAExecutionProvider', {
                            'device_id': 0,
                            'arena_extend_strategy': 'kNextPowerOfTwo',
                            'gpu_mem_limit': 2 * 1024 * 1024 * 1024,  # 2GB limit
                            'cudnn_conv_algo_search': 'EXHAUSTIVE',
                            'do_copy_in_default_stream': True,
                        }),
                        'CPUExecutionProvider'
                    ]
                    print(f"Loading ONNX model with CUDA GPU...")
                elif PREFERRED_PROVIDER == 'DmlExecutionProvider':
                    providers = [
                        'DmlExecutionProvider',
                        'CPUExecutionProvider'
                    ]
                    print(f"Loading ONNX model with DirectML GPU...")
                else:
                    providers = ['CPUExecutionProvider']
                    print(f"Loading ONNX model with CPU...")
                
                # Session options for optimization
                sess_options = ort.SessionOptions()
                sess_options.graph_optimization_level = ort.GraphOptimizationLevel.ORT_ENABLE_ALL
                sess_options.intra_op_num_threads = 4
                sess_options.inter_op_num_threads = 4
                
                self._session = ort.InferenceSession(
                    str(onnx_path), 
                    sess_options=sess_options,
                    providers=providers
                )
                self._use_onnx = True
                self.__class__._load_error = None
                
                # Get input/output info
                self._input_name = self._session.get_inputs()[0].name
                self._input_shape = self._session.get_inputs()[0].shape
                self._output_names = [o.name for o in self._session.get_outputs()]
                
                print(f"[OK] ONNX model loaded: {onnx_path}")
                print(f"  Input: {self._input_name} {self._input_shape}")
                print(f"  Provider: {self._session.get_providers()}")
                return
                
            except Exception as e:
                onnx_err = str(e)
                print(f"Error loading ONNX model: {onnx_err}")
                load_errors.append(f"ONNX load failed: {onnx_err}")
                self._session = None
        
        # Fallback to YOLO (.pt model) – try even when onnxruntime is installed
        if pt_path.exists():
            try:
                from ultralytics import YOLO
                self._model = YOLO(str(pt_path))
                self._use_onnx = False
                self.__class__._load_error = None
                print(f"Loaded YOLO model (fallback): {pt_path}")
                return
            except Exception as e:
                yolo_err = str(e)
                print(f"Error loading YOLO model: {yolo_err}")
                load_errors.append(f"YOLO load failed: {yolo_err}")
                self._model = None
        
        if not load_errors:
            load_errors.append(
                f"No model file found. Expected best.onnx or best.pt in: {models_dir}"
            )
        self.__class__._load_error = " | ".join(load_errors)
        print(f"No model available. {self.__class__._load_error}")
    
    # ==================== ONNX INFERENCE ====================
    
    def _preprocess_for_onnx(self, image: np.ndarray) -> Tuple[np.ndarray, dict]:
        """
        Preprocess image for ONNX model input
        Returns: (preprocessed tensor, metadata for coordinate recovery)
        """
        original_h, original_w = image.shape[:2]
        target_size = self.TARGET_SIZE
        
        # Calculate scale to fit in target size (letterbox)
        scale = min(target_size / original_w, target_size / original_h)
        new_w, new_h = int(original_w * scale), int(original_h * scale)
        
        # Resize
        resized = cv2.resize(image, (new_w, new_h), interpolation=cv2.INTER_LINEAR)
        
        # Create letterbox (pad to square)
        pad_x = (target_size - new_w) // 2
        pad_y = (target_size - new_h) // 2
        
        letterboxed = np.full((target_size, target_size, 3), 114, dtype=np.uint8)
        letterboxed[pad_y:pad_y + new_h, pad_x:pad_x + new_w] = resized
        
        # Convert BGR to RGB
        rgb = cv2.cvtColor(letterboxed, cv2.COLOR_BGR2RGB)
        
        # Normalize to 0-1 and transpose to NCHW format
        tensor = rgb.astype(np.float32) / 255.0
        tensor = np.transpose(tensor, (2, 0, 1))  # HWC -> CHW
        tensor = np.expand_dims(tensor, axis=0)   # Add batch dimension
        
        metadata = {
            "scale": scale,
            "pad_x": pad_x,
            "pad_y": pad_y,
            "original_w": original_w,
            "original_h": original_h
        }
        
        return tensor, metadata
    
    def _postprocess_onnx(self, outputs: np.ndarray, metadata: dict, 
                          confidence: float = 0.25) -> List[dict]:
        """
        Post-process ONNX model outputs to get detections (vectorized).
        YOLOv8 ONNX output shape: [1, 5, 8400] or [1, num_classes+4, 8400]
        """
        output = outputs[0]
        
        # Remove batch dimension if present -> [5, 8400]
        if len(output.shape) == 3:
            output = output[0]
        
        # Transpose to [8400, 5]
        output = output.T
        
        scale = metadata["scale"]
        pad_x = metadata["pad_x"]
        pad_y = metadata["pad_y"]
        orig_w = metadata["original_w"]
        orig_h = metadata["original_h"]
        num_cols = output.shape[1]
        
        # Extract confidence scores (vectorized)
        if num_cols == 5:
            conf_scores = output[:, 4]
            cls_ids = np.zeros(len(output), dtype=np.int32)
        else:
            class_scores = output[:, 4:]
            cls_ids = np.argmax(class_scores, axis=1)
            conf_scores = np.max(class_scores, axis=1)
        
        # Filter by confidence (boolean mask)
        mask = conf_scores >= confidence
        if not np.any(mask):
            print(f"ONNX postprocess: {len(output)} anchors -> 0 detections (conf>={confidence})")
            return []
        
        filtered = output[mask]
        filtered_conf = conf_scores[mask]
        filtered_cls = cls_ids[mask]
        
        # Convert center/wh to corner coordinates (vectorized)
        x1 = (filtered[:, 0] - filtered[:, 2] / 2 - pad_x) / scale
        y1 = (filtered[:, 1] - filtered[:, 3] / 2 - pad_y) / scale
        x2 = (filtered[:, 0] + filtered[:, 2] / 2 - pad_x) / scale
        y2 = (filtered[:, 1] + filtered[:, 3] / 2 - pad_y) / scale
        
        # Clip to image bounds (vectorized)
        np.clip(x1, 0, orig_w, out=x1)
        np.clip(y1, 0, orig_h, out=y1)
        np.clip(x2, 0, orig_w, out=x2)
        np.clip(y2, 0, orig_h, out=y2)
        
        # Valid detection filter (x2 > x1 and y2 > y1)
        valid = (x2 > x1) & (y2 > y1)
        x1, y1, x2, y2 = x1[valid], y1[valid], x2[valid], y2[valid]
        filtered_conf = filtered_conf[valid]
        filtered_cls = filtered_cls[valid]
        
        # Build detection list
        bboxes = np.stack([x1, y1, x2, y2], axis=1).astype(np.int32)
        detections = [
            {"bbox": bboxes[i].tolist(), "confidence": float(filtered_conf[i]), "class": int(filtered_cls[i])}
            for i in range(len(bboxes))
        ]
        
        print(f"ONNX postprocess: {len(output)} anchors -> {len(detections)} detections (conf>={confidence})")
        return detections
    
    def _run_onnx_inference(self, image: np.ndarray, confidence: float = 0.25) -> List[dict]:
        """Run inference using ONNX Runtime"""
        # Preprocess
        tensor, metadata = self._preprocess_for_onnx(image)
        
        # Run inference
        outputs = self._session.run(self._output_names, {self._input_name: tensor})
        
        # Post-process
        detections = self._postprocess_onnx(outputs, metadata, confidence)
        
        return detections
    
    def _nms(self, detections: List[dict], iou_threshold: float = 0.5) -> List[dict]:
        """Apply Non-Maximum Suppression"""
        if len(detections) == 0:
            return []
        
        # Sort by confidence
        sorted_dets = sorted(detections, key=lambda x: x["confidence"], reverse=True)
        
        kept = []
        for det in sorted_dets:
            x1, y1, x2, y2 = det["bbox"]
            
            is_duplicate = False
            for kept_det in kept:
                kx1, ky1, kx2, ky2 = kept_det["bbox"]
                
                # Calculate IOU
                inter_x1 = max(x1, kx1)
                inter_y1 = max(y1, ky1)
                inter_x2 = min(x2, kx2)
                inter_y2 = min(y2, ky2)
                
                if inter_x2 > inter_x1 and inter_y2 > inter_y1:
                    inter_area = (inter_x2 - inter_x1) * (inter_y2 - inter_y1)
                    area1 = (x2 - x1) * (y2 - y1)
                    area2 = (kx2 - kx1) * (ky2 - ky1)
                    union_area = area1 + area2 - inter_area
                    iou = inter_area / union_area if union_area > 0 else 0
                    
                    if iou > iou_threshold:
                        is_duplicate = True
                        break
            
            if not is_duplicate:
                kept.append(det)
        
        return kept
    
    # ==================== IMAGE PREPROCESSING ====================
    
    def letterbox(self, image: np.ndarray, target_size: int = None) -> Tuple[np.ndarray, float, Tuple[int, int]]:
        """Resize image with letterboxing to maintain aspect ratio"""
        if target_size is None:
            target_size = self.TARGET_SIZE
            
        h, w = image.shape[:2]
        scale = min(target_size / w, target_size / h)
        new_w, new_h = int(w * scale), int(h * scale)
        
        resized = cv2.resize(image, (new_w, new_h), interpolation=cv2.INTER_LINEAR)
        letterboxed = np.full((target_size, target_size, 3), 114, dtype=np.uint8)
        
        pad_x = (target_size - new_w) // 2
        pad_y = (target_size - new_h) // 2
        letterboxed[pad_y:pad_y + new_h, pad_x:pad_x + new_w] = resized
        
        return letterboxed, scale, (pad_x, pad_y)
    
    def apply_clahe(self, image: np.ndarray) -> np.ndarray:
        """Apply CLAHE for contrast enhancement"""
        lab = cv2.cvtColor(image, cv2.COLOR_BGR2LAB)
        l, a, b = cv2.split(lab)
        clahe = cv2.createCLAHE(clipLimit=self.CLAHE_CLIP_LIMIT, tileGridSize=self.CLAHE_TILE_SIZE)
        l_enhanced = clahe.apply(l)
        lab_enhanced = cv2.merge([l_enhanced, a, b])
        return cv2.cvtColor(lab_enhanced, cv2.COLOR_LAB2BGR)
    
    def normalize_color(self, image: np.ndarray) -> np.ndarray:
        """Normalize color space"""
        if len(image.shape) == 2:
            image = cv2.cvtColor(image, cv2.COLOR_GRAY2BGR)
        elif image.shape[2] == 4:
            image = cv2.cvtColor(image, cv2.COLOR_BGRA2BGR)
        return cv2.GaussianBlur(image, (3, 3), 0)
    
    def preprocess_image(self, image: np.ndarray, apply_letterbox: bool = False) -> Tuple[np.ndarray, Optional[dict]]:
        """Full preprocessing pipeline"""
        normalized = self.normalize_color(image)
        enhanced = self.apply_clahe(normalized)
        
        metadata = None
        if apply_letterbox:
            enhanced, scale, padding = self.letterbox(enhanced)
            metadata = {"scale": scale, "padding": padding}
        
        return enhanced, metadata
    
    # ==================== POST-PROCESSING FILTERS ====================
    
    def filter_by_area(self, detections: List[dict], image_shape: Tuple[int, int]) -> List[dict]:
        """Filter detections by bounding box area"""
        h, w = image_shape[:2]
        image_area = h * w
        min_area = image_area * self.MIN_AREA_RATIO
        max_area = image_area * self.MAX_AREA_RATIO
        
        filtered = []
        for det in detections:
            x1, y1, x2, y2 = det["bbox"]
            box_w = x2 - x1
            box_h = y2 - y1
            area = box_w * box_h
            
            if area < min_area or area > max_area:
                continue
            
            aspect_ratio = box_w / max(box_h, 1)
            if aspect_ratio < self.MIN_ASPECT_RATIO or aspect_ratio > self.MAX_ASPECT_RATIO:
                continue
            
            filtered.append(det)
        
        return filtered
    
    def filter_by_center_distance(self, detections: List[dict]) -> List[dict]:
        """Filter out phantom detections by center distance.
        Uses per-pair max(radius) threshold instead of global average."""
        if len(detections) <= 1:
            return detections
        
        n = len(detections)
        
        # Extract data to numpy arrays
        bboxes = np.array([det["bbox"] for det in detections])
        confidences = np.array([det.get("confidence", 0) for det in detections])
        
        # Calculate centers and radii
        cx = (bboxes[:, 0] + bboxes[:, 2]) / 2
        cy = (bboxes[:, 1] + bboxes[:, 3]) / 2
        radii = np.maximum(bboxes[:, 2] - bboxes[:, 0], bboxes[:, 3] - bboxes[:, 1]) / 2
        
        # Sort by confidence descending
        sorted_indices = np.argsort(-confidences)
        
        # Greedy selection with per-pair max radius threshold
        kept_mask = np.zeros(n, dtype=bool)
        kept_cx = []
        kept_cy = []
        kept_radii = []
        
        for idx in sorted_indices:
            if len(kept_cx) == 0:
                kept_mask[idx] = True
                kept_cx.append(cx[idx])
                kept_cy.append(cy[idx])
                kept_radii.append(radii[idx])
            else:
                # Calculate distances to all kept centers
                kept_cx_arr = np.array(kept_cx)
                kept_cy_arr = np.array(kept_cy)
                kept_radii_arr = np.array(kept_radii)
                distances = np.sqrt((cx[idx] - kept_cx_arr)**2 + (cy[idx] - kept_cy_arr)**2)
                
                # Per-pair threshold: use the LARGER radius of the two detections
                thresholds = np.maximum(radii[idx], kept_radii_arr) * self.CENTER_DISTANCE_RATIO
                
                if np.all(distances >= thresholds):
                    kept_mask[idx] = True
                    kept_cx.append(cx[idx])
                    kept_cy.append(cy[idx])
                    kept_radii.append(radii[idx])
        
        kept_count = np.sum(kept_mask)
        print(f"Center distance filter: {n} -> {kept_count} (removed {n - kept_count})")
        
        return [detections[i] for i in range(n) if kept_mask[i]]
    
    def filter_small_circles(self, detections: List[dict]) -> List[dict]:
        """Remove circles significantly smaller than average"""
        if len(detections) <= 3:
            return detections
        
        radii = []
        for det in detections:
            x1, y1, x2, y2 = det["bbox"]
            radii.append(max(x2 - x1, y2 - y1) / 2)
        
        avg_radius = sum(radii) / len(radii)
        min_allowed_radius = avg_radius * self.MIN_RADIUS_RATIO
        
        filtered = [det for i, det in enumerate(detections) if radii[i] >= min_allowed_radius]
        
        removed = len(detections) - len(filtered)
        if removed > 0:
            print(f"Small circle filter: removed {removed} circles")
        
        return filtered
    
    def filter_isolated_detections(self, detections: List[dict], min_neighbors: int = 2) -> List[dict]:
        """Remove isolated detections (e.g. scratches on table surface).
        Real batteries in a tray have many neighbors; false positives on the table are isolated."""
        if len(detections) <= 5:
            return detections
        
        bboxes = np.array([det["bbox"] for det in detections])
        cx = (bboxes[:, 0] + bboxes[:, 2]) / 2
        cy = (bboxes[:, 1] + bboxes[:, 3]) / 2
        radii = np.maximum(bboxes[:, 2] - bboxes[:, 0], bboxes[:, 3] - bboxes[:, 1]) / 2
        
        avg_radius = np.mean(radii)
        neighbor_radius = avg_radius * 3.0  # Search within ~1.5 battery diameters
        
        # Count neighbors for each detection
        n = len(detections)
        kept = []
        
        for i in range(n):
            distances = np.sqrt((cx[i] - cx)**2 + (cy[i] - cy)**2)
            # Count how many other detections are within neighbor_radius (exclude self)
            neighbor_count = np.sum((distances > 0) & (distances < neighbor_radius))
            
            if neighbor_count >= min_neighbors:
                kept.append(detections[i])
        
        removed = n - len(kept)
        if removed > 0:
            print(f"Isolated detection filter: removed {removed} detections (min_neighbors={min_neighbors})")
        
        return kept
    
    def filter_concentric_detections(self, detections: List[dict], iou_threshold: float = 0.5) -> List[dict]:
        """Filter out concentric/overlapping detections"""
        if len(detections) <= 1:
            return detections
        
        circles = []
        for i, det in enumerate(detections):
            x1, y1, x2, y2 = det["bbox"]
            cx = (x1 + x2) / 2
            cy = (y1 + y2) / 2
            radius = max(x2 - x1, y2 - y1) / 2
            circles.append({"index": i, "cx": cx, "cy": cy, "radius": radius, "det": det})
        
        circles.sort(key=lambda c: c["radius"], reverse=True)
        kept = []
        
        for circle in circles:
            is_concentric = False
            for kept_circle in kept:
                dist = np.sqrt((circle["cx"] - kept_circle["cx"])**2 + 
                             (circle["cy"] - kept_circle["cy"])**2)
                if dist < min(circle["radius"], kept_circle["radius"]) * 0.5:
                    is_concentric = True
                    break
            
            if not is_concentric:
                kept.append(circle)
        
        return [c["det"] for c in kept]
    
    def filter_edge_duplicates(self, detections: List[dict]) -> List[dict]:
        """Remove edge detections whose center falls inside another detection's bbox.
        Targets edge fragments where a partial detection overlaps with the true battery detection."""
        if len(detections) <= 1:
            return detections
        
        n = len(detections)
        bboxes = np.array([det["bbox"] for det in detections])
        confidences = np.array([det.get("confidence", 0) for det in detections])
        
        # Calculate centers
        cx = (bboxes[:, 0] + bboxes[:, 2]) / 2
        cy = (bboxes[:, 1] + bboxes[:, 3]) / 2
        
        suppressed = set()
        
        for i in range(n):
            if i in suppressed:
                continue
            for j in range(n):
                if j == i or j in suppressed:
                    continue
                
                # Check if center of j falls inside bbox of i
                if (bboxes[i, 0] <= cx[j] <= bboxes[i, 2] and
                    bboxes[i, 1] <= cy[j] <= bboxes[i, 3]):
                    # Both centers inside each other's bbox -> suppress the one with lower confidence
                    # Or if j's center is inside i's bbox, suppress the smaller/lower-confidence one
                    if (bboxes[j, 0] <= cx[i] <= bboxes[j, 2] and
                        bboxes[j, 1] <= cy[i] <= bboxes[j, 3]):
                        # Mutual overlap - suppress lower confidence
                        if confidences[j] < confidences[i]:
                            suppressed.add(j)
                        else:
                            suppressed.add(i)
                            break
                    else:
                        # j's center inside i's bbox but not vice versa -> j is likely edge fragment
                        suppressed.add(j)
        
        filtered = [detections[i] for i in range(n) if i not in suppressed]
        removed = n - len(filtered)
        if removed > 0:
            print(f"Edge duplicate filter: removed {removed} edge detections")
        return filtered
    
    # ==================== SAHI (Slicing Aided Hyper Inference) ====================
    
    def get_slices(self, image_shape: Tuple[int, int], overlap_ratio: float = None) -> List[Tuple[int, int, int, int]]:
        """Calculate slice coordinates for SAHI - ensures full image coverage
        
        Args:
            image_shape: (height, width) of the image
            overlap_ratio: Override overlap ratio (if None, uses adaptive)
        """
        h, w = image_shape[:2]
        slice_size = self.SAHI_SLICE_SIZE
        
        if overlap_ratio is None:
            overlap_ratio = self.get_adaptive_overlap(image_shape)
        
        overlap = int(slice_size * overlap_ratio)
        step = slice_size - overlap
        
        slices = []
        for y in range(0, h, step):
            for x in range(0, w, step):
                x2 = min(x + slice_size, w)
                y2 = min(y + slice_size, h)
                
                # If edge slice is too small, expand it backward instead of skipping
                if x2 - x < slice_size // 2:
                    x = max(0, x2 - slice_size)
                if y2 - y < slice_size // 2:
                    y = max(0, y2 - slice_size)
                    
                slices.append((x, y, x2, y2))
        
        # Remove duplicate slices
        slices = list(set(slices))
        return slices
    
    def merge_detections(self, all_detections: List[dict], iou_threshold: float = 0.5) -> List[dict]:
        """Merge overlapping detections from different slices.
        Uses BOTH IoU and per-pair center-distance to catch all duplicates."""
        if len(all_detections) <= 1:
            return all_detections
        
        # First pass: standard NMS by IoU
        after_nms = self._nms(all_detections, iou_threshold)
        
        # Second pass: distance-based merge for remaining duplicates
        # Uses per-pair max(radius) for more accurate duplicate detection
        if len(after_nms) <= 1:
            return after_nms
        
        bboxes = np.array([d["bbox"] for d in after_nms])
        confidences = np.array([d.get("confidence", 0) for d in after_nms])
        
        cx = (bboxes[:, 0] + bboxes[:, 2]) / 2
        cy = (bboxes[:, 1] + bboxes[:, 3]) / 2
        radii = np.maximum(bboxes[:, 2] - bboxes[:, 0], bboxes[:, 3] - bboxes[:, 1]) / 2
        
        # Greedy merge: keep highest confidence, suppress nearby
        order = np.argsort(-confidences)
        keep = []
        suppressed = set()
        
        for i in order:
            if int(i) in suppressed:
                continue
            keep.append(int(i))
            for j in order:
                if int(j) in suppressed or int(j) == int(i):
                    continue
                dist = np.sqrt((cx[i] - cx[j])**2 + (cy[i] - cy[j])**2)
                # Per-pair threshold: use LARGER radius of the two detections
                pair_threshold = max(radii[i], radii[j]) * 0.85
                if dist < pair_threshold:
                    suppressed.add(int(j))
        
        merged = [after_nms[i] for i in keep]
        if len(after_nms) != len(merged):
            print(f"Distance merge: {len(after_nms)} -> {len(merged)} (removed {len(after_nms) - len(merged)} duplicates)")
        return merged
    
    def detect_with_sahi(self, image: np.ndarray, confidence: float = 0.3) -> Tuple[int, List[dict]]:
        """Detect using SAHI with batch processing for faster inference"""
        h, w = image.shape[:2]
        slices = self.get_slices((h, w))
        
        print(f"SAHI: Processing {len(slices)} slices for {w}x{h} image (batch_size={self.BATCH_SIZE})")
        
        all_detections = []
        t0 = time.time()
        
        if self._use_onnx and self._session:
            # Batch processing for ONNX
            all_detections = self._sahi_batch_onnx(image, slices, confidence)
        else:
            # Sequential processing for YOLO fallback
            for x1, y1, x2, y2 in slices:
                slice_img = image[y1:y2, x1:x2]
                processed_slice, _ = self.preprocess_image(slice_img, apply_letterbox=False)
                
                if self._model:
                    results = self._model(processed_slice, conf=confidence, 
                                         iou=self.DEFAULT_IOU_THRESHOLD, verbose=False)
                    for result in results:
                        for box in result.boxes:
                            bx1, by1, bx2, by2 = box.xyxy[0].cpu().numpy()
                            all_detections.append({
                                "bbox": [int(bx1 + x1), int(by1 + y1), int(bx2 + x1), int(by2 + y1)],
                                "confidence": float(box.conf[0]),
                                "class": int(box.cls[0])
                            })
        
        t1 = time.time()
        print(f"SAHI Inference: {t1-t0:.3f}s")
        
        # Merge and filter
        merged = self.merge_detections(all_detections, iou_threshold=self.SAHI_MERGE_IOU)
        filtered = self.filter_by_area(merged, (h, w))
        filtered = self.filter_concentric_detections(filtered)
        filtered = self.filter_edge_duplicates(filtered)
        filtered = self.filter_by_center_distance(filtered)
        filtered = self.filter_small_circles(filtered)
        filtered = self.filter_isolated_detections(filtered)
        
        print(f"SAHI: {len(all_detections)} raw -> {len(merged)} merged -> {len(filtered)} final")
        
        return len(filtered), filtered
    
    def _sahi_batch_onnx(self, image: np.ndarray, slices: List[Tuple[int, int, int, int]], 
                         confidence: float) -> List[dict]:
        """Process SAHI slices in batches for ONNX Runtime"""
        all_detections = []
        
        # Prepare all slices
        slice_data = []  # [(tensor, metadata, x1, y1), ...]
        for x1, y1, x2, y2 in slices:
            slice_img = image[y1:y2, x1:x2]
            processed_slice, _ = self.preprocess_image(slice_img, apply_letterbox=False)
            tensor, metadata = self._preprocess_for_onnx(processed_slice)
            slice_data.append((tensor, metadata, x1, y1))
        
        # Process in batches
        for batch_start in range(0, len(slice_data), self.BATCH_SIZE):
            batch_end = min(batch_start + self.BATCH_SIZE, len(slice_data))
            batch = slice_data[batch_start:batch_end]
            
            # Stack tensors for batch inference
            batch_tensors = np.concatenate([s[0] for s in batch], axis=0)
            
            # Run batch inference
            outputs = self._session.run(self._output_names, {self._input_name: batch_tensors})
            
            # Process each result in batch
            batch_output = outputs[0]  # Shape: [batch_size, 5, 8400]
            
            for i, (_, metadata, offset_x, offset_y) in enumerate(batch):
                # Get single output from batch
                single_output = batch_output[i:i+1]  # Keep batch dim for postprocess
                
                # Postprocess
                dets = self._postprocess_onnx([single_output], metadata, confidence)
                
                # Add offset to convert to original image coordinates
                for det in dets:
                    det["bbox"][0] += offset_x
                    det["bbox"][1] += offset_y
                    det["bbox"][2] += offset_x
                    det["bbox"][3] += offset_y
                    all_detections.append(det)
        
        return all_detections
    
    def _quick_detect(self, image: np.ndarray) -> Tuple[int, List[dict]]:
        """Quick detection for density estimation"""
        try:
            processed, _ = self.preprocess_image(image, apply_letterbox=False)
            
            if self._use_onnx and self._session:
                detections = self._run_onnx_inference(processed, self.STANDARD_CONFIDENCE)
                detections = self._nms(detections, self.DEFAULT_IOU_THRESHOLD)
            elif self._model:
                results = self._model(processed, conf=self.STANDARD_CONFIDENCE, 
                                      iou=self.DEFAULT_IOU_THRESHOLD, verbose=False)
                detections = []
                for result in results:
                    for box in result.boxes:
                        x1, y1, x2, y2 = box.xyxy[0].cpu().numpy()
                        detections.append({
                            "bbox": [int(x1), int(y1), int(x2), int(y2)],
                            "confidence": float(box.conf[0]),
                            "class": int(box.cls[0])
                        })
            else:
                return 0, []
            
            return len(detections), detections
        except Exception as e:
            print(f"Quick detect error: {e}")
            return 0, []
    
    def detect_batteries(self, image: np.ndarray, confidence: float = None, 
                          use_preprocessing: bool = True, use_sahi: bool = None) -> Tuple[int, List[dict]]:
        """
        Detect batteries with ONNX Runtime GPU acceleration
        """
        if confidence is None:
            confidence = self.DEFAULT_CONFIDENCE
            
        if self._session is None and self._model is None:
            print("No model loaded – cannot detect batteries.")
            return 0, []
        
        try:
            h, w = image.shape[:2]
            max_dim = max(w, h)
            
            t_start = time.time()
            
            # Auto-adjust confidence based on image quality
            adaptive_confidence = self.get_adaptive_confidence(image)
            
            # Skip PASS 1 for large images if configured (production mode)
            if use_sahi is None and self.SKIP_PASS1_FOR_LARGE_TRAYS and max_dim >= self.SAHI_MIN_IMAGE_SIZE:
                print(f"[SKIP PASS 1] Large image {w}x{h} -> Direct SAHI")
                use_sahi = True
                confidence = adaptive_confidence
            # Density-based two-pass detection (for mixed use cases)
            elif use_sahi is None and self.DENSITY_CHECK_ENABLED and self.SAHI_ENABLED:
                print(f"[PASS 1] Quick detection for {w}x{h} image...")
                quick_count, quick_detections = self._quick_detect(image)
                print(f"[PASS 1] Found {quick_count} objects ({time.time()-t_start:.3f}s)")
                
                if quick_count >= self.SAHI_MIN_COUNT and max_dim >= self.SAHI_MIN_IMAGE_SIZE:
                    print(f"[PASS 2] Using SAHI for {quick_count} objects")
                    use_sahi = True
                    confidence = adaptive_confidence
                else:
                    print(f"[RESULT] Standard detection: {quick_count} objects")
                    filtered = self.filter_by_area(quick_detections, (h, w))
                    filtered = self.filter_concentric_detections(filtered)
                    filtered = self.filter_by_center_distance(filtered)
                    print(f"Total time: {time.time()-t_start:.3f}s")
                    return len(filtered), filtered
            
            if use_sahi:
                count, dets = self.detect_with_sahi(image, confidence)
                print(f"Total time: {time.time()-t_start:.3f}s")
                return count, dets
            
            print(f"Running standard detection...")
            original_shape = image.shape
            
            if use_preprocessing:
                processed_image, _ = self.preprocess_image(image, apply_letterbox=False)
            else:
                processed_image = image
            
            # Use ONNX or YOLO
            if self._use_onnx and self._session:
                detections = self._run_onnx_inference(processed_image, confidence)
                detections = self._nms(detections, self.DEFAULT_IOU_THRESHOLD)
            elif self._model:
                results = self._model(processed_image, conf=confidence, 
                                      iou=self.DEFAULT_IOU_THRESHOLD, verbose=False)
                detections = []
                for result in results:
                    for box in result.boxes:
                        x1, y1, x2, y2 = box.xyxy[0].cpu().numpy()
                        detections.append({
                            "bbox": [int(x1), int(y1), int(x2), int(y2)],
                            "confidence": float(box.conf[0]),
                            "class": int(box.cls[0])
                        })
            else:
                detections = []
            
            detections = self.filter_by_area(detections, original_shape)
            
            print(f"Total time: {time.time()-t_start:.3f}s")
            return len(detections), detections
            
        except Exception as e:
            print(f"Detection error: {e}")
            import traceback
            traceback.print_exc()
            return 0, []
    
    def _mock_detection(self, image: np.ndarray) -> Tuple[int, List[dict]]:
        """Mock detection for testing"""
        h, w = image.shape[:2]
        import random
        count = random.randint(5, 20)
        detections = []
        
        for i in range(count):
            x1 = random.randint(50, w - 150)
            y1 = random.randint(50, h - 150)
            x2 = x1 + random.randint(50, 100)
            y2 = y1 + random.randint(50, 100)
            
            detections.append({
                "bbox": [x1, y1, x2, y2],
                "confidence": random.uniform(0.7, 0.99),
                "class": 0
            })
        
        return count, detections
    
    def draw_circles(self, image: np.ndarray, detections: List[dict], already_filtered: bool = True, po_number: str = None) -> np.ndarray:
        """Draw green circles on detected batteries (detections assumed already filtered)"""
        result_image = image.copy()
        
        # Skip filtering if already done (SAHI path)
        if already_filtered:
            filtered_detections = detections
        else:
            # Apply filters for non-SAHI path
            filtered_detections = self.filter_concentric_detections(detections)
            filtered_detections = self.filter_by_center_distance(filtered_detections)
        
        # Sort for consistent numbering
        def get_center(det):
            x1, y1, x2, y2 = det["bbox"]
            return ((y1 + y2) / 2, (x1 + x2) / 2)
        
        sorted_detections = sorted(filtered_detections, key=get_center)
        
        for det in sorted_detections:
            x1, y1, x2, y2 = det["bbox"]
            cx = int((x1 + x2) / 2)
            cy = int((y1 + y2) / 2)
            # Draw small red dot at center instead of green circle
            cv2.circle(result_image, (cx, cy), 5, (0, 0, 255), -1)  # Red filled dot, radius 5
        
        # Build overlay text lines
        font = cv2.FONT_HERSHEY_SIMPLEX
        font_scale = 1.0
        thickness = 2
        line_gap = 10
        
        lines = []  # (text, color)
        if po_number:
            lines.append((f"PO: {po_number}", (0, 255, 255)))  # Yellow
        lines.append((f"Total: {len(sorted_detections)}", (0, 255, 0)))  # Green
        lines.append((datetime.now().strftime("%d/%m/%Y %H:%M:%S"), (255, 255, 255)))  # White
        
        # Calculate sizes
        line_sizes = [cv2.getTextSize(text, font, font_scale, thickness) for text, _ in lines]
        max_w = max(size[0][0] for size in line_sizes)
        total_h = sum(size[0][1] for size in line_sizes) + line_gap * (len(lines) - 1)
        
        # Background rectangle
        cv2.rectangle(result_image, (10, 10), (max_w + 30, total_h + 40), (0, 0, 0), -1)
        
        # Draw each line
        y_offset = 20
        for i, ((text, color), (size, _)) in enumerate(zip(lines, line_sizes)):
            y_offset += size[1]
            cv2.putText(result_image, text, (20, y_offset), font, font_scale, color, thickness)
            y_offset += line_gap
        
        return result_image, len(sorted_detections)
    
    def draw_boxes(self, image: np.ndarray, detections: List[dict], po_number: str = None) -> np.ndarray:
        """Draw circles (legacy compatibility)"""
        result_image, _ = self.draw_circles(image, detections, po_number=po_number)
        return result_image
    
    def process_image(self, image_bytes: bytes, confidence: float = 0.5, po_number: str = None) -> dict:
        """Full processing pipeline - optimized"""
        if not self.is_model_loaded:
            return {
                "error": (
                    "No AI model loaded. "
                    "Please place best.onnx (or best.pt) in the models/ directory "
                    "and restart the service."
                ),
                "count": 0,
            }

        nparr = np.frombuffer(image_bytes, np.uint8)
        image = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        
        if image is None:
            return {"error": "Failed to decode image", "count": 0}
        
        # Detection already includes all filtering
        count, detections = self.detect_batteries(image, confidence)
        
        # Draw circles - detections already filtered, no need to re-filter
        result_image, _ = self.draw_circles(image, detections, already_filtered=True, po_number=po_number)
        
        # Encode result image
        _, buffer = cv2.imencode('.jpg', result_image, [cv2.IMWRITE_JPEG_QUALITY, 85])
        result_base64 = base64.b64encode(buffer).decode('utf-8')
        
        return {
            "count": count,
            "detections": detections,
            "result_image": result_base64
        }
    
    def save_result_image(self, image_bytes: bytes, detections: List[dict], 
                          output_dir: str = "static/results", filepath: str = None, po_number: str = None) -> str:
        """Save annotated result image to disk
        
        Args:
            filepath: If provided, save to this exact path instead of generating new name
            po_number: Purchase Order number to overlay on image
        """
        nparr = np.frombuffer(image_bytes, np.uint8)
        image = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        result_image = self.draw_boxes(image, detections, po_number=po_number)
        
        if filepath:
            # Use provided filepath
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            cv2.imwrite(filepath, result_image)
            return filepath
        else:
            # Generate new filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            filename = f"result_{timestamp}.jpg"
            os.makedirs(output_dir, exist_ok=True)
            full_path = os.path.join(output_dir, filename)
            cv2.imwrite(full_path, result_image)
            return full_path


# Global AI engine instance
ai_engine = AIEngine()
