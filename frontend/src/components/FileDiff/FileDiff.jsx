import React, { useEffect, useRef, useState } from 'react';
import { Alert, Typography } from 'antd';
import * as Diff2Html from 'diff2html';
import 'diff2html/bundles/css/diff2html.min.css';

const { Text } = Typography;

export default function FileDiff({ diff, isBinary, isOfficeExtracted, fromVersion, toVersion, isExpanded }) {
  const containerRef = useRef(null);
  const [, forceUpdate] = useState(0);

  useEffect(() => {
    if (!diff || isBinary) return;

    if (!containerRef.current) {
      const timer = setTimeout(() => forceUpdate(n => n + 1), 50);
      return () => clearTimeout(timer);
    }

    const html = Diff2Html.html(diff, {
      drawFileList: false,
      matching: 'lines',
      outputFormat: 'line-by-line',
      renderNothingWhenEmpty: false,
    });

    containerRef.current.innerHTML = html;
  }, [diff, isBinary, containerRef.current]);

  if (isBinary) {
    return (
      <div>
        <Alert
          type="info"
          message="Binary file comparison"
          description="Content diff is not available for binary files. You can download both versions to compare manually."
          showIcon
        />
        <div style={{ marginTop: 16, display: 'flex', gap: 32 }}>
          <div>
            <Text strong>From: v{fromVersion?.versionNumber}</Text>
            <br />
            <Text type="secondary">Size: {formatBytes(fromVersion?.size)}</Text>
            <br />
            <Text type="secondary" style={{ fontSize: 11, fontFamily: 'monospace' }}>
              SHA256: {fromVersion?.checksum?.substring(0, 16)}...
            </Text>
          </div>
          <div>
            <Text strong>To: v{toVersion?.versionNumber}</Text>
            <br />
            <Text type="secondary">Size: {formatBytes(toVersion?.size)}</Text>
            <br />
            <Text type="secondary" style={{ fontSize: 11, fontFamily: 'monospace' }}>
              SHA256: {toVersion?.checksum?.substring(0, 16)}...
            </Text>
          </div>
        </div>
      </div>
    );
  }

  if (!diff) return null;

  return (
    <div>
      {isOfficeExtracted && (
        <Alert
          type="warning"
          message="📄 Nội dung được trích xuất từ file Office để so sánh. Một số định dạng (bảng, hình ảnh, biểu đồ) có thể không hiển thị."
          showIcon
          style={{ marginBottom: 12 }}
        />
      )}
      <div
        ref={containerRef}
        style={{ fontSize: 13, overflow: isExpanded ? 'visible' : 'auto' }}
        className={isExpanded ? 'diff-container diff-container-expanded' : 'diff-container'}
      />
    </div>
  );
}

function formatBytes(bytes) {
  if (!bytes) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  return `${(bytes / Math.pow(1024, i)).toFixed(1)} ${units[i]}`;
}