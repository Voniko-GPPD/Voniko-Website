import React, { useState } from 'react';
import { Alert, Button, Col, DatePicker, Form, Input, Row, Space, Table, Typography } from 'antd';
import { ReloadOutlined, SearchOutlined } from '@ant-design/icons';
import dayjs from 'dayjs';
import { fetchBatches } from '../../../api/dmpApi';
import { useLang } from '../../../contexts/LangContext';

const dateFormat = 'YYYY-MM-DD';

export default function DMPFilterPanel({ stationId, selectedBatchId, onSelect }) {
  const { t } = useLang();
  const [form] = Form.useForm();
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [batches, setBatches] = useState([]);
  const [searched, setSearched] = useState(false);
  const [pagination, setPagination] = useState({ current: 1, pageSize: 50 });

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
          || String(b.fdrq || '').toLowerCase().includes(keyword),
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

  const columns = [
    {
      title: '#',
      key: 'idx',
      width: 60,
      render: (_v, _r, i) => (pagination.current - 1) * pagination.pageSize + i + 1,
    },
    { title: t('dm2000StartDate'), dataIndex: 'fdrq', key: 'fdrq', width: 140 },
    { title: t('dm2000Type'), dataIndex: 'dcxh', key: 'dcxh', width: 140 },
    { title: t('dmpBatchId'), dataIndex: 'id', key: 'id', width: 180 },
    {
      title: t('dmpPattern'),
      dataIndex: 'fdfs',
      key: 'fdfs',
      render: (value) => value || '-',
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
    </Space>
  );
}
