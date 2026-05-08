import React, { useState } from 'react';
import { Alert, Button, Col, DatePicker, Form, Input, Modal, Row, Space, Table, Tooltip, Typography } from 'antd';
import { EditOutlined, ReloadOutlined, SearchOutlined } from '@ant-design/icons';
import dayjs from 'dayjs';
import { fetchBatches, saveDmpBatchOverride } from '../../../api/dmpApi';
import { useLang } from '../../../contexts/LangContext';

const dateFormat = 'YYYY-MM-DD';

export default function DMPFilterPanel({ stationId, selectedBatchId, onSelect }) {
  const { t } = useLang();
  const [form] = Form.useForm();
  const [editForm] = Form.useForm();
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [batches, setBatches] = useState([]);
  const [searched, setSearched] = useState(false);
  const [pagination, setPagination] = useState({ current: 1, pageSize: 50 });
  const [editingBatch, setEditingBatch] = useState(null);
  const [editSaving, setEditSaving] = useState(false);

  const onSearch = async () => {
    if (!stationId) return;
    const values = form.getFieldsValue();
    const keyword = values.keyword?.trim().toLowerCase() || '';
    const dateFrom = values.date_from ? dayjs(values.date_from).format(dateFormat) : null;
    const dateTo = values.date_to ? dayjs(values.date_to).format(dateFormat) : null;

    setLoading(true);
    setError('');
    setSearched(true);
    try {
      const result = await fetchBatches(stationId);
      let filtered = result || [];
      if (dateFrom) filtered = filtered.filter((b) => !b.fdrq || b.fdrq >= dateFrom);
      if (dateTo) filtered = filtered.filter((b) => !b.fdrq || b.fdrq <= dateTo);
      if (keyword) {
        filtered = filtered.filter((b) =>
          String(b.id || '').toLowerCase().includes(keyword)
          || String(b.dcxh || '').toLowerCase().includes(keyword)
          || String(b.fdfs || '').toLowerCase().includes(keyword)
          || String(b.fdrq || '').toLowerCase().includes(keyword)
          || String(b.name || '').toLowerCase().includes(keyword)
          || String(b.manufacturer || '').toLowerCase().includes(keyword)
          || String(b.archname || '').toLowerCase().includes(keyword)
          || String(b.serialno || '').toLowerCase().includes(keyword)
          || String(b.remarks || '').toLowerCase().includes(keyword),
        );
      }
      setBatches(filtered);
      setPagination((prev) => ({ ...prev, current: 1 }));
    } catch (err) {
      setError(err.message || 'Failed to load batches');
      setBatches([]);
    } finally {
      setLoading(false);
    }
  };

  const onReset = () => {
    form.resetFields();
    setSearched(false);
    setError('');
    setBatches([]);
    setPagination((prev) => ({ ...prev, current: 1 }));
    onSelect?.(null);
  };

  const openEditModal = (e, record) => {
    e.stopPropagation();
    setEditingBatch(record);
    editForm.setFieldsValue({
      serialno: record.serialno || '',
      remarks: record.remarks || '',
    });
  };

  const handleEditSave = async () => {
    if (!editingBatch || !stationId) return;
    const values = editForm.getFieldsValue();
    setEditSaving(true);
    try {
      await saveDmpBatchOverride(stationId, String(editingBatch.id), {
        serialno: values.serialno?.trim() || null,
        remarks: values.remarks?.trim() || null,
      });
      // Update local state so the table shows the new values immediately
      setBatches((prev) =>
        prev.map((b) =>
          b.id === editingBatch.id
            ? { ...b, serialno: values.serialno?.trim() || null, remarks: values.remarks?.trim() || null }
            : b
        )
      );
      setEditingBatch(null);
    } catch (err) {
      setError(err.message || 'Failed to save');
    } finally {
      setEditSaving(false);
    }
  };

  const EditableCell = ({ value, record }) => (
    <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
      <span style={{ flex: 1, overflow: 'hidden', textOverflow: 'ellipsis' }}>{value || '-'}</span>
      <Tooltip title={t('dm2000EditOverride')}>
        <Button
          size="small"
          type="text"
          icon={<EditOutlined style={{ fontSize: 11, color: '#999' }} />}
          style={{ flexShrink: 0, padding: '0 2px', height: 18, minWidth: 18 }}
          onClick={(e) => openEditModal(e, record)}
        />
      </Tooltip>
    </div>
  );

  const columns = [
    {
      title: '#',
      key: 'idx',
      width: 60,
      render: (_v, _r, i) => (pagination.current - 1) * pagination.pageSize + i + 1,
    },
    {
      title: t('dm2000File'),
      dataIndex: 'id',
      key: 'id',
      width: 160,
      ellipsis: true,
      render: (v) => v || '-',
    },
    { title: t('dm2000StartDate'), dataIndex: 'fdrq', key: 'fdrq', width: 120, render: (v) => v || '-' },
    { title: t('dm2000Type'), dataIndex: 'dcxh', key: 'dcxh', width: 120, render: (v) => v || '-' },
    { title: t('dm2000Name'), dataIndex: 'name', key: 'name', width: 200, ellipsis: true, render: (v) => v || '-' },
    {
      title: t('dm2000DisCondition'),
      dataIndex: 'fdfs',
      key: 'fdfs',
      width: 240,
      render: (v) => v || '-',
    },
    { title: t('dmpChannelCount'), dataIndex: 'channel_count', key: 'channel_count', width: 90, align: 'center', render: (v) => (v != null ? v : '-') },
    { title: t('dm2000Manufacturer'), dataIndex: 'manufacturer', key: 'manufacturer', width: 120, render: (v) => v || '-' },
    { title: t('dm2000MadeDate'), dataIndex: 'madedate', key: 'madedate', width: 110, render: (v) => v || '-' },
    {
      title: t('dm2000SerialNo'),
      dataIndex: 'serialno',
      key: 'serialno',
      width: 140,
      render: (v, record) => <EditableCell value={v} record={record} />,
    },
    {
      title: t('dm2000Remarks'),
      dataIndex: 'remarks',
      key: 'remarks',
      width: 160,
      ellipsis: true,
      render: (v, record) => <EditableCell value={v} record={record} />,
    },
    { title: t('dm2000Database'), dataIndex: 'database', key: 'database', width: 320, ellipsis: true, render: (v) => v || '-' },
  ];

  return (
    <Space direction="vertical" size={12} style={{ width: '100%' }}>
      <Form form={form} layout="vertical" size="small">
        <Row gutter={[16, 0]}>
          <Col xs={24} sm={12} md={8} lg={6} xl={4}>
            <Form.Item name="date_from" label={t('dm2000DateFrom')} style={{ marginBottom: 8 }}>
              <DatePicker style={{ width: '100%' }} />
            </Form.Item>
          </Col>
          <Col xs={24} sm={12} md={8} lg={6} xl={4}>
            <Form.Item name="date_to" label={t('dm2000DateTo')} style={{ marginBottom: 8 }}>
              <DatePicker style={{ width: '100%' }} />
            </Form.Item>
          </Col>
          <Col xs={24} sm={12} md={8} lg={6} xl={8}>
            <Form.Item name="keyword" label={t('dm2000KeywordFilter')} style={{ marginBottom: 8 }}>
              <Input allowClear placeholder={t('dmpKeywordFilterPlaceholder')} />
            </Form.Item>
          </Col>
        </Row>
        <Space>
          <Button
            type="primary"
            icon={<SearchOutlined />}
            onClick={onSearch}
            disabled={!stationId}
            loading={loading}
          >
            {t('dm2000Search')}
          </Button>
          <Button icon={<ReloadOutlined />} onClick={onReset}>
            {t('dm2000Reset')}
          </Button>
        </Space>
      </Form>

      {error && <Alert type="error" message={error} showIcon />}

      {searched && (
        <>
          <Typography.Text type="secondary">{t('dmpTotal', { count: batches.length })}</Typography.Text>
          <Table
            size="small"
            rowKey="id"
            columns={columns}
            dataSource={batches}
            loading={loading}
            pagination={{
              current: pagination.current,
              pageSize: pagination.pageSize,
              showSizeChanger: true,
              onChange: (page, pageSize) => setPagination({ current: page, pageSize }),
            }}
            scroll={{ x: 'max-content', y: 500 }}
            onRow={(record) => ({
              onClick: () => onSelect?.(record),
              style: {
                cursor: 'pointer',
                ...(record.id === selectedBatchId ? { backgroundColor: '#e6f4ff' } : {}),
              },
            })}
          />
        </>
      )}

      <Modal
        open={!!editingBatch}
        title={t('dmpEditBatchMetaTitle', { batchId: editingBatch?.id || '' })}
        onCancel={() => setEditingBatch(null)}
        onOk={handleEditSave}
        okText={t('dm2000Save')}
        cancelText={t('dm2000Cancel')}
        confirmLoading={editSaving}
        width={420}
        destroyOnClose
      >
        <Form form={editForm} layout="vertical" style={{ marginTop: 16 }}>
          <Form.Item name="serialno" label={t('dm2000SerialNo')}>
            <Input placeholder={t('dm2000SerialNoPlaceholder')} allowClear />
          </Form.Item>
          <Form.Item name="remarks" label={t('dm2000Remarks')}>
            <Input.TextArea rows={3} placeholder={t('dm2000RemarksPlaceholder')} allowClear />
          </Form.Item>
        </Form>
      </Modal>
    </Space>
  );
}
