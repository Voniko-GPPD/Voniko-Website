import React, { useRef, useState } from 'react';
import { Alert, Button, Col, DatePicker, Form, Input, Modal, Row, Space, Table, Tooltip, Typography } from 'antd';
import { EditOutlined, ReloadOutlined, SearchOutlined, SyncOutlined } from '@ant-design/icons';
import dayjs from 'dayjs';
import { fetchDM2000Archives, refreshDM2000Archives, saveDM2000ArchiveOverride } from '../../../api/dm2000Api';
import { useLang } from '../../../contexts/LangContext';

const dateFormat = 'YYYY-MM-DD';

export default function DM2000FilterPanel({ stationId, selectedArchname, onSelect }) {
  const { t } = useLang();
  const [form] = Form.useForm();
  const [editForm] = Form.useForm();
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [archives, setArchives] = useState([]);
  const [total, setTotal] = useState(0);
  const [searched, setSearched] = useState(false);
  const [pagination, setPagination] = useState({ current: 1, pageSize: 50 });
  const [reloading, setReloading] = useState(false);
  const [editingArchive, setEditingArchive] = useState(null);
  const [editSaving, setEditSaving] = useState(false);
  const lastFiltersRef = useRef({});

  const onSearch = async (filters) => {
    if (!stationId) return;
    const values = filters || form.getFieldsValue();
    const queryFilters = {
      date_from: values.date_from ? dayjs(values.date_from).format(dateFormat) : undefined,
      date_to: values.date_to ? dayjs(values.date_to).format(dateFormat) : undefined,
      type_filter: values.type_filter?.trim() || undefined,
      name_filter: values.name_filter?.trim() || undefined,
      mfr_filter: values.mfr_filter?.trim() || undefined,
      keyword: values.keyword?.trim() || undefined,
    };
    lastFiltersRef.current = values;

    setLoading(true);
    setError('');
    setSearched(true);
    try {
      const result = await fetchDM2000Archives(stationId, queryFilters);
      setArchives(result.archives || []);
      setTotal(result.total || 0);
      setPagination((prev) => ({ ...prev, current: 1 }));
    } catch (err) {
      setError(err.message || 'Failed to load archives');
      setArchives([]);
      setTotal(0);
    } finally {
      setLoading(false);
    }
  };

  const onReset = () => {
    form.resetFields();
    setSearched(false);
    setError('');
    setArchives([]);
    setTotal(0);
    setPagination((prev) => ({ ...prev, current: 1 }));
    onSelect?.(null);
  };

  const onReloadData = async () => {
    if (!stationId) return;
    setReloading(true);
    setError('');
    try {
      await refreshDM2000Archives(stationId);
      if (searched) await onSearch(lastFiltersRef.current);
    } catch (err) {
      setError(err.message || 'Reload failed');
    } finally {
      setReloading(false);
    }
  };

  const openEditModal = (e, record) => {
    e.stopPropagation();
    setEditingArchive(record);
    editForm.setFieldsValue({
      serialno: record.serialno || '',
      remarks: record.remarks || '',
    });
  };

  const handleEditSave = async () => {
    if (!editingArchive || !stationId) return;
    const values = editForm.getFieldsValue();
    setEditSaving(true);
    try {
      await saveDM2000ArchiveOverride(stationId, editingArchive.archname, {
        serialno: values.serialno?.trim() || null,
        remarks: values.remarks?.trim() || null,
      });
      // Update local state so the table shows the new values immediately
      setArchives((prev) =>
        prev.map((a) =>
          a.archname === editingArchive.archname
            ? { ...a, serialno: values.serialno?.trim() || null, remarks: values.remarks?.trim() || null }
            : a
        )
      );
      setEditingArchive(null);
    } catch (err) {
      setError(err.message || 'Failed to save');
    } finally {
      setEditSaving(false);
    }
  };

  const EditableCell = ({ value, record, field }) => (
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
      title: t('dm2000File'),
      dataIndex: 'archname',
      key: 'archname_file',
      width: 120,
      ellipsis: true,
      render: (value) => value || '-',
    },
    { title: t('dm2000StartDate'), dataIndex: 'startdate', key: 'startdate', width: 140, render: (v) => v || '-' },
    { title: t('dm2000Type'), dataIndex: 'dcxh', key: 'dcxh', width: 120, render: (v) => v || '-' },
    { title: t('dm2000Name'), dataIndex: 'name', key: 'name', width: 140, render: (v) => v || '-' },
    {
      title: t('dm2000DisCondition'),
      key: 'dis_condition',
      width: 240,
      render: (_, record) => {
        const resistance = String(record.load_resistance || '').trim();
        const fdfs = String(record.fdfs || '').trim();
        const endpoint = String(record.endpoint_voltage || '').trim();
        const prefix = [resistance, fdfs].filter(Boolean).join(',');
        const suffix = endpoint ? ` to ${endpoint}V` : '';
        const full = prefix + suffix;
        return full || '-';
      },
    },
    { title: t('dm2000Duration'), dataIndex: 'duration', key: 'duration', width: 120, render: (v) => (v != null && v !== '') ? String(v) : '-' },
    { title: t('dm2000UnifRate'), dataIndex: 'unifrate', key: 'unifrate', width: 100, render: (v) => (v != null && v !== '') ? String(v) : '-' },
    { title: t('dm2000Manufacturer'), dataIndex: 'manufacturer', key: 'manufacturer', width: 140, render: (v) => v || '-' },
    { title: t('dm2000MadeDate'), dataIndex: 'madedate', key: 'madedate', width: 120, render: (v) => v || '-' },
    { title: t('dm2000ArchName'), dataIndex: 'archname', key: 'archname', width: 160, render: (v) => v || '-' },
    {
      title: t('dm2000SerialNo'),
      dataIndex: 'serialno',
      key: 'serialno',
      width: 160,
      render: (v, record) => <EditableCell value={v} record={record} field="serialno" />,
    },
    {
      title: t('dm2000Remarks'),
      dataIndex: 'remarks',
      key: 'remarks',
      width: 180,
      render: (v, record) => <EditableCell value={v} record={record} field="remarks" />,
    },
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
          <Col xs={24} sm={12} md={8} lg={6} xl={4}>
            <Form.Item name="type_filter" label={t('dm2000TypeFilter')} style={{ marginBottom: 8 }}>
              <Input allowClear />
            </Form.Item>
          </Col>
          <Col xs={24} sm={12} md={8} lg={6} xl={4}>
            <Form.Item name="name_filter" label={t('dm2000NameFilter')} style={{ marginBottom: 8 }}>
              <Input allowClear />
            </Form.Item>
          </Col>
          <Col xs={24} sm={12} md={8} lg={6} xl={4}>
            <Form.Item name="mfr_filter" label={t('dm2000MfrFilter')} style={{ marginBottom: 8 }}>
              <Input allowClear />
            </Form.Item>
          </Col>
          <Col xs={24} sm={12} md={8} lg={6} xl={8}>
            <Form.Item name="keyword" label={t('dm2000KeywordFilter')} style={{ marginBottom: 8 }}>
              <Input allowClear placeholder={t('dm2000KeywordFilterPlaceholder')} />
            </Form.Item>
          </Col>
        </Row>
        <Space>
          <Button type="primary" icon={<SearchOutlined />} onClick={() => onSearch()} disabled={!stationId} loading={loading}>
            {t('dm2000Search')}
          </Button>
          <Button icon={<ReloadOutlined />} onClick={onReset}>
            {t('dm2000Reset')}
          </Button>
          <Button icon={<SyncOutlined />} onClick={onReloadData} disabled={!stationId} loading={reloading}>
            {t('dm2000ReloadData')}
          </Button>
        </Space>
      </Form>

      {error && <Alert type="error" message={error} showIcon />}

      {searched && (
        <>
          <Typography.Text type="secondary">{t('dm2000Total', { count: total })}</Typography.Text>
          <Table
            size="small"
            rowKey="archname"
            columns={columns}
            dataSource={archives}
            loading={loading}
            pagination={{
              current: pagination.current,
              pageSize: pagination.pageSize,
              showSizeChanger: true,
              onChange: (page, pageSize) => setPagination({ current: page, pageSize }),
            }}
            scroll={{ x: 2000, y: 500 }}
            onRow={(record) => ({
              onClick: () => onSelect?.(record),
              style: {
                cursor: 'pointer',
                ...(record.archname === selectedArchname ? { backgroundColor: '#e6f4ff' } : {}),
              },
            })}
          />
        </>
      )}

      <Modal
        open={!!editingArchive}
        title={t('dm2000EditOverrideTitle', { archname: editingArchive?.archname || '' })}
        onCancel={() => setEditingArchive(null)}
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
