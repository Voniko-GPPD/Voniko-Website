import React, { useState } from 'react';
import { Alert, Button, Col, DatePicker, Form, Input, Row, Space, Table, Typography } from 'antd';
import { ReloadOutlined, SearchOutlined } from '@ant-design/icons';
import dayjs from 'dayjs';
import { fetchDM2000Archives } from '../../../api/dm2000Api';
import { useLang } from '../../../contexts/LangContext';

const dateFormat = 'YYYY-MM-DD';

export default function DM2000FilterPanel({ stationId, selectedArchname, onSelect }) {
  const { t } = useLang();
  const [form] = Form.useForm();
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [archives, setArchives] = useState([]);
  const [total, setTotal] = useState(0);
  const [searched, setSearched] = useState(false);

  const onSearch = async () => {
    if (!stationId) return;
    const values = form.getFieldsValue();
    const filters = {
      date_from: values.date_from ? dayjs(values.date_from).format(dateFormat) : undefined,
      date_to: values.date_to ? dayjs(values.date_to).format(dateFormat) : undefined,
      type_filter: values.type_filter?.trim() || undefined,
      name_filter: values.name_filter?.trim() || undefined,
      mfr_filter: values.mfr_filter?.trim() || undefined,
      serial_filter: values.serial_filter?.trim() || undefined,
    };

    setLoading(true);
    setError('');
    setSearched(true);
    try {
      const result = await fetchDM2000Archives(stationId, filters);
      setArchives(result.archives || []);
      setTotal(result.total || 0);
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
    onSelect?.(null);
  };

  const columns = [
    { title: '#', key: 'idx', width: 60, render: (_v, _r, i) => i + 1 },
    { title: t('dm2000StartDate'), dataIndex: 'startdate', key: 'startdate', width: 140 },
    { title: t('dm2000Type'), dataIndex: 'dcxh', key: 'dcxh', width: 120 },
    { title: t('dm2000Name'), dataIndex: 'name', key: 'name', width: 140 },
    { title: t('dm2000DisCondition'), dataIndex: 'fdfs', key: 'fdfs', width: 130 },
    { title: t('dm2000Duration'), dataIndex: 'duration', key: 'duration', width: 120 },
    { title: t('dm2000UnifRate'), dataIndex: 'unifrate', key: 'unifrate', width: 100 },
    { title: t('dm2000Manufacturer'), dataIndex: 'manufacturer', key: 'manufacturer', width: 140 },
    { title: t('dm2000MadeDate'), dataIndex: 'madedate', key: 'madedate', width: 120 },
    { title: t('dm2000ArchName'), dataIndex: 'archname', key: 'archname', width: 160 },
    { title: t('dm2000SerialNo'), dataIndex: 'serialno', key: 'serialno', width: 140 },
    { title: t('dm2000Remarks'), dataIndex: 'remarks', key: 'remarks', width: 160 },
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
          <Col xs={24} sm={12} md={8} lg={6} xl={4}>
            <Form.Item name="serial_filter" label={t('dm2000SerialFilter')} style={{ marginBottom: 8 }}>
              <Input allowClear />
            </Form.Item>
          </Col>
        </Row>
        <Space>
          <Button type="primary" icon={<SearchOutlined />} onClick={onSearch} disabled={!stationId} loading={loading}>
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
          <Typography.Text type="secondary">{t('dm2000Total', { count: total })}</Typography.Text>
          <Table
            size="small"
            rowKey="archname"
            columns={columns}
            dataSource={archives}
            loading={loading}
            pagination={{ pageSize: 50 }}
            scroll={{ x: 'max-content', y: 500 }}
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
    </Space>
  );
}
