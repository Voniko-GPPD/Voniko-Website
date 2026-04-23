import React, { useState, useEffect, useRef, useCallback } from 'react';
import {
  Card, Row, Col, Tabs, Upload, Button, Slider, Input, Form, Spin,
  Table, DatePicker, Space, Statistic, Select, Modal, Popconfirm,
  Tag, Typography, Alert, Badge, notification, Tooltip, Empty,
} from 'antd';
import {
  InboxOutlined, CameraOutlined, ScanOutlined, DownloadOutlined,
  DeleteOutlined, ReloadOutlined, ExportOutlined, SearchOutlined,
  CheckCircleOutlined, CloseCircleOutlined, NumberOutlined, CalendarOutlined,
  UserOutlined, BarChartOutlined,
} from '@ant-design/icons';
import dayjs from 'dayjs';
import { useAuth } from '../../contexts/AuthContext';
import { useLang } from '../../contexts/LangContext';
import {
  predict, getHistory, getStats, exportExcel,
  deleteRecord, deleteBatch, checkHealth,
} from '../../api/countBatteries';

const { Title, Text } = Typography;
const { RangePicker } = DatePicker;
const { Dragger } = Upload;

// How many rows per page in history table
const PAGE_SIZE = 20;

export default function CountBatteriesPage() {
  const { user, isAdmin } = useAuth();
  const { t, lang } = useLang();

  // ─── Service health ─────────────────────────────────────────────────────────
  const [serviceOnline, setServiceOnline] = useState(null); // null=checking
  const [modelLoaded, setModelLoaded] = useState(null); // null=checking

  const checkServiceHealth = useCallback(async () => {
    try {
      const res = await checkHealth();
      setServiceOnline(true);
      setModelLoaded(res.data?.model_loaded === true);
    } catch {
      setServiceOnline(false);
      setModelLoaded(null);
    }
  }, []);

  useEffect(() => {
    checkServiceHealth();
  }, [checkServiceHealth]);

  // ─── Stats ──────────────────────────────────────────────────────────────────
  const [stats, setStats] = useState(null);
  const [statsLoading, setStatsLoading] = useState(false);

  const loadStats = useCallback(async () => {
    setStatsLoading(true);
    try {
      const res = await getStats();
      setStats(res.data);
    } catch {
      // ignore – service may be offline
    } finally {
      setStatsLoading(false);
    }
  }, []);

  // ─── Prediction (Tab 1) ─────────────────────────────────────────────────────
  const [previewUrl, setPreviewUrl] = useState(null);
  const [selectedFile, setSelectedFile] = useState(null);
  const [confidence, setConfidence] = useState(0.5);
  const [saveResult, setSaveResult] = useState(true);
  const [poNumber, setPoNumber] = useState('');
  const [predicting, setPredicting] = useState(false);
  const [result, setResult] = useState(null); // { count, result_image, po_number }

  // Camera capture
  const [cameraOpen, setCameraOpen] = useState(false);
  const videoRef = useRef(null);
  const streamRef = useRef(null);

  const openCamera = async () => {
    setCameraOpen(true);
    try {
      const stream = await navigator.mediaDevices.getUserMedia({
        video: { facingMode: { ideal: 'environment' }, width: { ideal: 1920 } },
      });
      streamRef.current = stream;
      if (videoRef.current) videoRef.current.srcObject = stream;
    } catch (err) {
      notification.error({ message: t('cbCameraError'), description: err.message });
      setCameraOpen(false);
    }
  };

  const closeCamera = () => {
    if (streamRef.current) {
      streamRef.current.getTracks().forEach((t) => t.stop());
      streamRef.current = null;
    }
    setCameraOpen(false);
  };

  const capturePhoto = () => {
    const video = videoRef.current;
    if (!video) return;
    const canvas = document.createElement('canvas');
    canvas.width = video.videoWidth;
    canvas.height = video.videoHeight;
    canvas.getContext('2d').drawImage(video, 0, 0);
    canvas.toBlob((blob) => {
      const file = new File([blob], `capture_${Date.now()}.jpg`, { type: 'image/jpeg' });
      setSelectedFile(file);
      setPreviewUrl(URL.createObjectURL(blob));
      setResult(null);
      closeCamera();
    }, 'image/jpeg', 0.92);
  };

  const handleFileSelect = (file) => {
    setSelectedFile(file);
    setPreviewUrl(URL.createObjectURL(file));
    setResult(null);
    return false; // prevent auto-upload
  };

  const runDetection = async () => {
    if (!selectedFile) {
      notification.warning({ message: t('cbSelectImage') });
      return;
    }
    setPredicting(true);
    setResult(null);
    try {
      const res = await predict(selectedFile, confidence, saveResult, poNumber || null);
      setResult(res.data);
      if (saveResult) {
        loadStats();
        loadHistory();
      }
      notification.success({
        message: t('cbDetectionDone'),
        description: `${t('cbBatteryCount')}: ${res.data.count}`,
      });
    } catch (err) {
      const detail = err.response?.data?.detail || err.message;
      notification.error({ message: t('cbDetectionFailed'), description: detail });
    } finally {
      setPredicting(false);
    }
  };

  const clearPrediction = () => {
    setSelectedFile(null);
    setPreviewUrl(null);
    setResult(null);
  };

  // ─── History (Tab 2) ────────────────────────────────────────────────────────
  const [history, setHistory] = useState([]);
  const [historyTotal, setHistoryTotal] = useState(0);
  const [historyLoading, setHistoryLoading] = useState(false);
  const [page, setPage] = useState(1);
  const [dateRange, setDateRange] = useState(null);
  const [filterPo, setFilterPo] = useState('');
  const [filterUsername, setFilterUsername] = useState('');
  const [selectedRowKeys, setSelectedRowKeys] = useState([]);
  const [batchDeleting, setBatchDeleting] = useState(false);
  const [resultImageModal, setResultImageModal] = useState(null);

  const loadHistory = useCallback(async () => {
    setHistoryLoading(true);
    try {
      const params = {
        skip: (page - 1) * PAGE_SIZE,
        limit: PAGE_SIZE,
      };
      if (dateRange?.[0]) params.date_from = dayjs(dateRange[0]).format('YYYY-MM-DD');
      if (dateRange?.[1]) params.date_to = dayjs(dateRange[1]).format('YYYY-MM-DD');
      if (filterPo.trim()) params.po_number = filterPo.trim();
      if (isAdmin && filterUsername.trim()) params.username = filterUsername.trim();

      const res = await getHistory(params);
      setHistory(res.data);
      const total = parseInt(res.headers['x-total-count'] || '0', 10);
      setHistoryTotal(total);
    } catch {
      // service offline
    } finally {
      setHistoryLoading(false);
    }
  }, [page, dateRange, filterPo, filterUsername, isAdmin]);

  // Load both on mount and when filter changes
  useEffect(() => {
    loadStats();
    loadHistory();
  }, [loadStats, loadHistory]);

  const handleDeleteSingle = async (id) => {
    try {
      await deleteRecord(id);
      notification.success({ message: t('cbRecordDeleted') });
      loadHistory();
      loadStats();
    } catch (err) {
      notification.error({
        message: t('error'),
        description: err.response?.data?.detail || err.message,
      });
    }
  };

  const handleDeleteBatch = async () => {
    if (selectedRowKeys.length === 0) return;
    setBatchDeleting(true);
    try {
      await deleteBatch(selectedRowKeys);
      notification.success({
        message: t('cbBatchDeleted', { count: selectedRowKeys.length }),
      });
      setSelectedRowKeys([]);
      loadHistory();
      loadStats();
    } catch (err) {
      notification.error({
        message: t('error'),
        description: err.response?.data?.detail || err.message,
      });
    } finally {
      setBatchDeleting(false);
    }
  };

  const handleExport = async () => {
    try {
      const params = {};
      if (dateRange?.[0]) params.date_from = dayjs(dateRange[0]).format('YYYY-MM-DD');
      if (dateRange?.[1]) params.date_to = dayjs(dateRange[1]).format('YYYY-MM-DD');
      if (isAdmin && filterUsername.trim()) params.username = filterUsername.trim();

      const res = await exportExcel(params);
      const url = URL.createObjectURL(new Blob([res.data]));
      const a = document.createElement('a');
      a.href = url;
      a.download = `battery_count_${dayjs().format('YYYYMMDD_HHmm')}.xlsx`;
      a.click();
      URL.revokeObjectURL(url);
    } catch (err) {
      notification.error({ message: t('error'), description: err.message });
    }
  };

  // ─── Table columns ───────────────────────────────────────────────────────────
  const columns = [
    {
      title: 'ID',
      dataIndex: 'id',
      width: 70,
      sorter: (a, b) => b.id - a.id,
    },
    {
      title: t('cbBatteryCount'),
      dataIndex: 'count',
      width: 120,
      render: (v) => (
        <Tag color="green" style={{ fontSize: 14, padding: '2px 10px' }}>
          {v}
        </Tag>
      ),
      sorter: (a, b) => a.count - b.count,
    },
    {
      title: 'PO',
      dataIndex: 'po_number',
      width: 140,
      render: (v) => v ? <Tag color="blue">{v}</Tag> : <Text type="secondary">—</Text>,
    },
    ...(isAdmin
      ? [
          {
            title: t('user'),
            dataIndex: 'username',
            width: 130,
            render: (v) => v || '—',
          },
        ]
      : []),
    {
      title: t('createdAt'),
      dataIndex: 'created_at',
      width: 160,
      render: (v) => v ? dayjs(v).format('DD/MM/YYYY HH:mm') : '—',
      sorter: (a, b) => new Date(b.created_at) - new Date(a.created_at),
    },
    {
      title: t('cbResultImage'),
      dataIndex: 'result_image_path',
      width: 100,
      render: (_, record) =>
        record.result_image_path ? (
          <Button
            size="small"
            type="link"
            onClick={() => setResultImageModal(record)}
          >
            {t('viewDetails')}
          </Button>
        ) : (
          <Text type="secondary">—</Text>
        ),
    },
    {
      title: t('actions'),
      width: 80,
      render: (_, record) => (
        <Popconfirm
          title={t('cbConfirmDelete')}
          onConfirm={() => handleDeleteSingle(record.id)}
          okText={t('yes')}
          cancelText={t('cancel')}
        >
          <Button size="small" danger icon={<DeleteOutlined />} />
        </Popconfirm>
      ),
    },
  ];

  // ─── Render ──────────────────────────────────────────────────────────────────
  const serviceStatusBadge = serviceOnline === null
    ? <Badge status="processing" text={t('loading')} />
    : serviceOnline
    ? modelLoaded === false
      ? <Badge status="warning" text={t('cbServiceNoModel')} />
      : <Badge status="success" text={t('cbServiceOnline')} />
    : <Badge status="error" text={t('cbServiceOffline')} />;

  return (
    <div style={{ maxWidth: 1200, margin: '0 auto' }}>
      {/* Page title */}
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 16 }}>
        <Title level={3} style={{ margin: 0 }}>
          🔋 {t('countBatteries')}
        </Title>
        <Space>
          {serviceStatusBadge}
          <Button icon={<ReloadOutlined />} onClick={() => { checkServiceHealth(); loadStats(); loadHistory(); }}>
            {t('refresh')}
          </Button>
        </Space>
      </div>

      {serviceOnline === false && (
        <Alert
          type="error"
          showIcon
          message={t('cbServiceOffline')}
          description={t('cbServiceOfflineDesc')}
          style={{ marginBottom: 16 }}
        />
      )}

      {serviceOnline && modelLoaded === false && (
        <Alert
          type="warning"
          showIcon
          message={t('cbServiceNoModel')}
          description={t('cbServiceNoModelDesc')}
          style={{ marginBottom: 16 }}
        />
      )}

      {/* Stats row */}
      <Row gutter={[16, 16]} style={{ marginBottom: 20 }}>
        {[
          { title: t('cbTotalDetections'), value: stats?.total_detections, icon: <ScanOutlined />, color: '#1677ff' },
          { title: t('cbTotalBatteries'), value: stats?.total_batteries, icon: <NumberOutlined />, color: '#52c41a' },
          { title: t('cbTodayDetections'), value: stats?.today_detections, icon: <CalendarOutlined />, color: '#fa8c16' },
          { title: t('cbTodayBatteries'), value: stats?.today_batteries, icon: <BarChartOutlined />, color: '#722ed1' },
        ].map((s) => (
          <Col xs={12} sm={6} key={s.title}>
            <Card size="small" loading={statsLoading} style={{ borderLeft: `3px solid ${s.color}` }}>
              <Statistic
                title={s.title}
                value={s.value ?? '—'}
                prefix={<span style={{ color: s.color }}>{s.icon}</span>}
              />
            </Card>
          </Col>
        ))}
      </Row>

      {/* Main tabs */}
      <Tabs
        defaultActiveKey="predict"
        items={[
          {
            key: 'predict',
            label: <span><ScanOutlined /> {t('cbDetectTab')}</span>,
            children: (
              <Row gutter={[16, 16]}>
                {/* Left: Upload + settings */}
                <Col xs={24} lg={12}>
                  <Card title={t('cbUploadImage')} size="small">
                    {/* Camera capture */}
                    <Space style={{ marginBottom: 12 }}>
                      <Button icon={<CameraOutlined />} onClick={openCamera}>
                        {t('cbOpenCamera')}
                      </Button>
                      {selectedFile && (
                        <Button danger onClick={clearPrediction}>
                          {t('cbClearImage')}
                        </Button>
                      )}
                    </Space>

                    {/* Drag-drop upload */}
                    <Dragger
                      accept="image/*"
                      beforeUpload={handleFileSelect}
                      showUploadList={false}
                      style={{ marginBottom: 12 }}
                    >
                      {previewUrl ? (
                        <img
                          src={previewUrl}
                          alt="preview"
                          style={{ maxWidth: '100%', maxHeight: 300, objectFit: 'contain' }}
                        />
                      ) : (
                        <>
                          <p className="ant-upload-drag-icon"><InboxOutlined /></p>
                          <p className="ant-upload-text">{t('cbDragOrClick')}</p>
                          <p className="ant-upload-hint">{t('cbImageHint')}</p>
                        </>
                      )}
                    </Dragger>

                    {/* Settings */}
                    <Form layout="vertical" size="small">
                      <Form.Item label={`${t('cbConfidence')}: ${(confidence * 100).toFixed(0)}%`}>
                        <Slider
                          min={0.1} max={0.9} step={0.05}
                          value={confidence}
                          onChange={setConfidence}
                          marks={{ 0.1: '10%', 0.5: '50%', 0.9: '90%' }}
                        />
                      </Form.Item>
                      <Form.Item label="PO Number">
                        <Input
                          placeholder={t('cbPoPlaceholder')}
                          value={poNumber}
                          onChange={(e) => setPoNumber(e.target.value)}
                          allowClear
                        />
                      </Form.Item>
                      <Form.Item>
                        <Space>
                          <input
                            type="checkbox"
                            id="saveResult"
                            checked={saveResult}
                            onChange={(e) => setSaveResult(e.target.checked)}
                          />
                          <label htmlFor="saveResult" style={{ cursor: 'pointer', userSelect: 'none' }}>
                            {t('cbSaveRecord')}
                          </label>
                        </Space>
                      </Form.Item>
                    </Form>

                    <Button
                      type="primary"
                      size="large"
                      icon={<ScanOutlined />}
                      loading={predicting}
                      disabled={!selectedFile || !serviceOnline || modelLoaded === false}
                      onClick={runDetection}
                      block
                    >
                      {predicting ? t('cbDetecting') : t('cbStartDetect')}
                    </Button>
                  </Card>
                </Col>

                {/* Right: Result */}
                <Col xs={24} lg={12}>
                  <Card title={t('cbResult')} size="small" style={{ minHeight: 300 }}>
                    {predicting && (
                      <div style={{ textAlign: 'center', padding: 40 }}>
                        <Spin size="large" tip={t('cbDetecting')} />
                      </div>
                    )}
                    {!predicting && !result && (
                      <Empty
                        description={t('cbNoResult')}
                        image={Empty.PRESENTED_IMAGE_SIMPLE}
                        style={{ padding: 40 }}
                      />
                    )}
                    {!predicting && result && (
                      <>
                        <div style={{ textAlign: 'center', marginBottom: 16 }}>
                          <Tag
                            color="green"
                            style={{ fontSize: 24, padding: '8px 24px', borderRadius: 8 }}
                          >
                            {result.count} {t('cbBatteries')}
                          </Tag>
                          {result.po_number && (
                            <Tag color="blue" style={{ marginLeft: 8, fontSize: 14 }}>
                              PO: {result.po_number}
                            </Tag>
                          )}
                        </div>
                        {result.result_image && (
                          <img
                            src={`data:image/jpeg;base64,${result.result_image}`}
                            alt="result"
                            style={{
                              width: '100%',
                              borderRadius: 8,
                              border: '1px solid #d9d9d9',
                              cursor: 'zoom-in',
                            }}
                            onClick={() => setResultImageModal({ result_image_b64: result.result_image })}
                          />
                        )}
                      </>
                    )}
                  </Card>
                </Col>
              </Row>
            ),
          },
          {
            key: 'history',
            label: <span><CalendarOutlined /> {t('cbHistoryTab')}</span>,
            children: (
              <Card size="small">
                {/* Filters */}
                <Row gutter={[12, 12]} style={{ marginBottom: 16 }}>
                  <Col xs={24} sm={12} lg={7}>
                    <RangePicker
                      style={{ width: '100%' }}
                      value={dateRange}
                      onChange={setDateRange}
                      placeholder={[t('cbDateFrom'), t('cbDateTo')]}
                    />
                  </Col>
                  <Col xs={24} sm={6} lg={4}>
                    <Input
                      placeholder="PO Number"
                      value={filterPo}
                      onChange={(e) => setFilterPo(e.target.value)}
                      allowClear
                      prefix={<SearchOutlined />}
                    />
                  </Col>
                  {isAdmin && (
                    <Col xs={24} sm={6} lg={4}>
                      <Input
                        placeholder={t('user')}
                        value={filterUsername}
                        onChange={(e) => setFilterUsername(e.target.value)}
                        allowClear
                        prefix={<UserOutlined />}
                      />
                    </Col>
                  )}
                  <Col xs={24} sm={12} lg={isAdmin ? 9 : 13}>
                    <Space wrap>
                      <Button icon={<ReloadOutlined />} onClick={loadHistory}>
                        {t('refresh')}
                      </Button>
                      {selectedRowKeys.length > 0 && (
                        <Popconfirm
                          title={t('cbConfirmBatchDelete', { count: selectedRowKeys.length })}
                          onConfirm={handleDeleteBatch}
                          okText={t('yes')}
                          cancelText={t('cancel')}
                        >
                          <Button
                            danger
                            icon={<DeleteOutlined />}
                            loading={batchDeleting}
                          >
                            {t('cbDeleteSelected')} ({selectedRowKeys.length})
                          </Button>
                        </Popconfirm>
                      )}
                      <Button icon={<ExportOutlined />} onClick={handleExport}>
                        {t('cbExportExcel')}
                      </Button>
                    </Space>
                  </Col>
                </Row>

                <Table
                  rowKey="id"
                  columns={columns}
                  dataSource={history}
                  loading={historyLoading}
                  rowSelection={{
                    selectedRowKeys,
                    onChange: setSelectedRowKeys,
                  }}
                  pagination={{
                    current: page,
                    pageSize: PAGE_SIZE,
                    total: historyTotal,
                    onChange: (p) => setPage(p),
                    showTotal: (total) => `${t('total')}: ${total}`,
                    showSizeChanger: false,
                  }}
                  scroll={{ x: 700 }}
                  size="small"
                />
              </Card>
            ),
          },
        ]}
      />

      {/* Camera modal */}
      <Modal
        open={cameraOpen}
        title={<><CameraOutlined /> {t('cbCapturePhoto')}</>}
        onCancel={closeCamera}
        footer={[
          <Button key="cancel" onClick={closeCamera}>{t('cancel')}</Button>,
          <Button key="capture" type="primary" icon={<CameraOutlined />} onClick={capturePhoto}>
            {t('cbCapture')}
          </Button>,
        ]}
        width={640}
        destroyOnClose
      >
        <video
          ref={videoRef}
          autoPlay
          playsInline
          muted
          style={{ width: '100%', borderRadius: 8, background: '#000' }}
        />
      </Modal>

      {/* Result image full-screen modal */}
      <Modal
        open={!!resultImageModal}
        title={
          resultImageModal
            ? `${t('cbResultImage')} — ${resultImageModal.count !== undefined ? `${resultImageModal.count} ${t('cbBatteries')}` : ''}`
            : ''
        }
        onCancel={() => setResultImageModal(null)}
        footer={null}
        width="90vw"
        style={{ top: 20 }}
        destroyOnClose
      >
        {resultImageModal && (
          <>
            {resultImageModal.result_image_b64 ? (
              <img
                src={`data:image/jpeg;base64,${resultImageModal.result_image_b64}`}
                alt="result"
                style={{ width: '100%' }}
              />
            ) : (
              <Alert
                type="info"
                message={t('cbResultImageUnavailable')}
                description={resultImageModal.result_image_path || ''}
              />
            )}
          </>
        )}
      </Modal>
    </div>
  );
}
