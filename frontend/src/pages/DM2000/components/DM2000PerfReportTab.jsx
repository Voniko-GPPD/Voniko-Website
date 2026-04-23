import React, { useState } from 'react';
import {
  Alert,
  Button,
  Card,
  Col,
  DatePicker,
  Empty,
  Form,
  Input,
  Row,
  Select,
  Space,
  Table,
  Tag,
  Tooltip,
  Typography,
  notification,
} from 'antd';
import {
  DeleteOutlined,
  DownloadOutlined,
  PlusOutlined,
  ReloadOutlined,
  SearchOutlined,
} from '@ant-design/icons';
import dayjs from 'dayjs';
import { fetchDM2000Archives, downloadDM2000PerfReport } from '../../../api/dm2000Api';
import { useLang } from '../../../contexts/LangContext';

const { Title, Text, Paragraph } = Typography;
const dateFormat = 'YYYY-MM-DD';

const BATTERY_TYPES = ['HP', 'UD', 'UD+'];

/** Auto-incrementing counter used to produce stable unique entry keys. */
let _entryCounter = 0;
const nextKey = () => { _entryCounter += 1; return `entry_${_entryCounter}`; };

/** Parse a range string like "1-4" or "1,2,3,4" into an array of ints. */
function parseBatys(str) {
  if (!str || !str.trim()) return [];
  const parts = str.split(',').flatMap((chunk) => {
    const m = chunk.trim().match(/^(\d+)-(\d+)$/);
    if (m) {
      const lo = parseInt(m[1], 10);
      const hi = parseInt(m[2], 10);
      return Array.from({ length: Math.max(0, hi - lo + 1) }, (_, i) => lo + i);
    }
    const n = parseInt(chunk.trim(), 10);
    return Number.isFinite(n) ? [n] : [];
  });
  return [...new Set(parts)].sort((a, b) => a - b);
}

export default function DM2000PerfReportTab({ stationId }) {
  const { t } = useLang();

  // — Step 1: archive search —
  const [searchForm] = Form.useForm();
  const [searchLoading, setSearchLoading] = useState(false);
  const [searchError, setSearchError] = useState('');
  const [archives, setArchives] = useState([]);
  const [searched, setSearched] = useState(false);
  const [selectedRowKeys, setSelectedRowKeys] = useState([]);

  // — Step 2: report entries —
  // entries: [{ key, archname, dcxh, serialno, startdate, fdfs, batteryType, batysStr, sheetName }]
  const [entries, setEntries] = useState([]);
  const [downloading, setDownloading] = useState(false);

  // ── Archive search ─────────────────────────────────────────────────────────
  const onSearch = async () => {
    if (!stationId) return;
    const values = searchForm.getFieldsValue();
    const filters = {
      date_from: values.date_from ? dayjs(values.date_from).format(dateFormat) : undefined,
      date_to: values.date_to ? dayjs(values.date_to).format(dateFormat) : undefined,
      keyword: values.keyword?.trim() || undefined,
    };
    setSearchLoading(true);
    setSearchError('');
    setSearched(true);
    try {
      const result = await fetchDM2000Archives(stationId, filters);
      setArchives(result.archives || []);
      setSelectedRowKeys([]);
    } catch (err) {
      setSearchError(err.message || 'Failed to load archives');
      setArchives([]);
    } finally {
      setSearchLoading(false);
    }
  };

  const onReset = () => {
    searchForm.resetFields();
    setSearched(false);
    setSearchError('');
    setArchives([]);
    setSelectedRowKeys([]);
  };

  // ── Add archives to report ─────────────────────────────────────────────────
  const makeDefaultSheetName = (record) => {
    const parts = [record.dcxh, record.serialno].filter(Boolean);
    return parts.join(' ').trim() || record.archname;
  };

  const addArchivesToReport = (records, defaultType = 'HP') => {
    const toAdd = [];
    records.forEach((record) => {
      const key = nextKey();
      toAdd.push({
        key,
        archname: record.archname,
        dcxh: record.dcxh || '',
        serialno: record.serialno || '',
        startdate: record.startdate || '',
        fdfs: record.fdfs || '',
        batteryType: defaultType,
        batysStr: '',
        sheetName: makeDefaultSheetName(record),
      });
    });
    if (toAdd.length > 0) {
      setEntries((prev) => [...prev, ...toAdd]);
      notification.success({
        message: `${toAdd.length} archive(s) added to report`,
        duration: 2,
      });
    }
  };

  const addSelectedToReport = () => {
    const selected = archives.filter((a) => selectedRowKeys.includes(a.archname));
    if (!selected.length) return;
    addArchivesToReport(selected, 'HP');
    setSelectedRowKeys([]);
  };

  const addSingleToReport = (record) => {
    addArchivesToReport([record], 'HP');
  };

  // ── Update / remove entry ─────────────────────────────────────────────────
  const updateEntry = (key, field, value) => {
    setEntries((prev) =>
      prev.map((e) => (e.key === key ? { ...e, [field]: value } : e))
    );
  };

  const removeEntry = (key) => {
    setEntries((prev) => prev.filter((e) => e.key !== key));
  };

  // ── Download report ────────────────────────────────────────────────────────
  const handleDownload = async () => {
    if (!stationId) {
      notification.warning({ message: t('dm2000PerfSelectStation') });
      return;
    }
    if (!entries.length) return;

    setDownloading(true);
    try {
      const payload = entries.map((e) => ({
        archname: e.archname,
        batteryType: e.batteryType,
        batys: parseBatys(e.batysStr),
        sheetName: e.sheetName || '',
      }));
      await downloadDM2000PerfReport({ stationId, entries: payload });
      notification.success({ message: t('dm2000PerfSuccess') });
    } catch (err) {
      notification.error({
        message: t('dm2000PerfError'),
        description: err.message,
      });
    } finally {
      setDownloading(false);
    }
  };

  // ── Search table columns ───────────────────────────────────────────────────
  const searchColumns = [
    { title: t('dm2000StartDate'), dataIndex: 'startdate', key: 'startdate', width: 120 },
    { title: t('dm2000Type'), dataIndex: 'dcxh', key: 'dcxh', width: 100 },
    { title: t('dm2000Name'), dataIndex: 'name', key: 'name', width: 130 },
    {
      title: t('dm2000PerfFdfs'),
      dataIndex: 'fdfs',
      key: 'fdfs',
      width: 200,
      render: (v) => v || '-',
    },
    { title: t('dm2000Manufacturer'), dataIndex: 'manufacturer', key: 'manufacturer', width: 120 },
    { title: t('dm2000SerialNo'), dataIndex: 'serialno', key: 'serialno', width: 100 },
    { title: t('dm2000ArchName'), dataIndex: 'archname', key: 'archname', width: 150 },
    {
      key: 'action',
      width: 120,
      render: (_, record) => (
        <Button
          size="small"
          icon={<PlusOutlined />}
          onClick={() => addSingleToReport(record)}
        >
          {t('dm2000PerfAddSingle')}
        </Button>
      ),
    },
  ];

  // ── Entry table columns ────────────────────────────────────────────────────
  const entryColumns = [
    {
      title: t('dm2000PerfArchiveName'),
      key: 'archname',
      width: 150,
      render: (_, e) => (
        <Space direction="vertical" size={0}>
          <Text strong style={{ fontSize: 12 }}>{e.archname}</Text>
          <Text type="secondary" style={{ fontSize: 11 }}>{e.startdate}</Text>
        </Space>
      ),
    },
    {
      title: t('dm2000PerfFdfs'),
      dataIndex: 'fdfs',
      key: 'fdfs',
      width: 180,
      render: (v) => <Tag color="blue">{v || '-'}</Tag>,
    },
    {
      title: t('dm2000PerfBatteryType'),
      key: 'batteryType',
      width: 120,
      render: (_, e) => (
        <Select
          size="small"
          style={{ width: 90 }}
          value={e.batteryType}
          onChange={(v) => updateEntry(e.key, 'batteryType', v)}
          options={BATTERY_TYPES.map((bt) => ({ value: bt, label: bt }))}
        />
      ),
    },
    {
      title: t('dm2000PerfBatys'),
      key: 'batysStr',
      width: 180,
      render: (_, e) => (
        <Tooltip title={t('dm2000PerfBatysTooltip')}>
          <Input
            size="small"
            placeholder={t('dm2000PerfAllBatys')}
            value={e.batysStr}
            onChange={(ev) => updateEntry(e.key, 'batysStr', ev.target.value)}
            style={{ width: 160 }}
          />
        </Tooltip>
      ),
    },
    {
      title: (
        <Tooltip title={t('dm2000PerfSheetNameHint')}>
          {t('dm2000PerfSheetName')}
        </Tooltip>
      ),
      key: 'sheetName',
      width: 160,
      render: (_, e) => (
        <Input
          size="small"
          value={e.sheetName}
          onChange={(ev) => updateEntry(e.key, 'sheetName', ev.target.value)}
          style={{ width: 140 }}
        />
      ),
    },
    {
      key: 'remove',
      width: 60,
      render: (_, e) => (
        <Button
          size="small"
          danger
          icon={<DeleteOutlined />}
          onClick={() => removeEntry(e.key)}
        />
      ),
    },
  ];

  return (
    <Space direction="vertical" size={16} style={{ width: '100%' }}>
      <Title level={5} style={{ marginTop: 0 }}>{t('dm2000PerfReportTitle')}</Title>
      <Paragraph type="secondary" style={{ marginBottom: 0 }}>
        {t('dm2000PerfReportDesc')}
      </Paragraph>

      {/* ── Step 1 ── */}
      <Card
        size="small"
        title={<Text strong>{t('dm2000PerfStep1')}</Text>}
        bodyStyle={{ paddingBottom: 8 }}
      >
        <Form form={searchForm} layout="vertical" size="small">
          <Row gutter={[16, 0]}>
            <Col xs={24} sm={12} md={6}>
              <Form.Item name="date_from" label={t('dm2000DateFrom')} style={{ marginBottom: 8 }}>
                <DatePicker style={{ width: '100%' }} />
              </Form.Item>
            </Col>
            <Col xs={24} sm={12} md={6}>
              <Form.Item name="date_to" label={t('dm2000DateTo')} style={{ marginBottom: 8 }}>
                <DatePicker style={{ width: '100%' }} />
              </Form.Item>
            </Col>
            <Col xs={24} sm={24} md={10}>
              <Form.Item name="keyword" label={t('dm2000KeywordFilter')} style={{ marginBottom: 8 }}>
                <Input allowClear placeholder={t('dm2000KeywordFilterPlaceholder')} />
              </Form.Item>
            </Col>
          </Row>
          <Space>
            <Button
              type="primary"
              icon={<SearchOutlined />}
              onClick={onSearch}
              disabled={!stationId}
              loading={searchLoading}
            >
              {t('dm2000Search')}
            </Button>
            <Button icon={<ReloadOutlined />} onClick={onReset}>
              {t('dm2000Reset')}
            </Button>
            {selectedRowKeys.length > 0 && (
              <Button
                icon={<PlusOutlined />}
                onClick={addSelectedToReport}
              >
                {t('dm2000PerfAddSelected')} ({selectedRowKeys.length})
              </Button>
            )}
          </Space>
        </Form>

        {searchError && <Alert type="error" message={searchError} showIcon style={{ marginTop: 8 }} />}

        {searched && (
          <div style={{ marginTop: 8 }}>
            <Text type="secondary" style={{ display: 'block', marginBottom: 4 }}>
              {t('dm2000Total', { count: archives.length })}
            </Text>
            <Table
              size="small"
              rowKey="archname"
              columns={searchColumns}
              dataSource={archives}
              loading={searchLoading}
              pagination={{ pageSize: 20, size: 'small' }}
              scroll={{ x: 'max-content', y: 300 }}
              rowSelection={{
                selectedRowKeys,
                onChange: setSelectedRowKeys,
              }}
            />
          </div>
        )}
      </Card>

      {/* ── Step 2 ── */}
      <Card
        size="small"
        title={<Text strong>{t('dm2000PerfStep2')}</Text>}
        extra={
          entries.length > 0 && (
            <Button
              type="primary"
              icon={<DownloadOutlined />}
              loading={downloading}
              onClick={handleDownload}
            >
              {downloading ? t('dm2000PerfDownloading') : t('dm2000PerfDownload')}
            </Button>
          )
        }
      >
        {entries.length === 0 ? (
          <Empty
            image={Empty.PRESENTED_IMAGE_SIMPLE}
            description={t('dm2000PerfNoEntries')}
          />
        ) : (
          <Space direction="vertical" size={8} style={{ width: '100%' }}>
            <Table
              size="small"
              rowKey="key"
              columns={entryColumns}
              dataSource={entries}
              pagination={false}
              scroll={{ x: 'max-content' }}
            />
            <Button
              type="primary"
              icon={<DownloadOutlined />}
              loading={downloading}
              onClick={handleDownload}
              style={{ marginTop: 4 }}
            >
              {downloading ? t('dm2000PerfDownloading') : t('dm2000PerfDownload')}
            </Button>
          </Space>
        )}
      </Card>
    </Space>
  );
}
