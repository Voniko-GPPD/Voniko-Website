import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  Alert,
  Button,
  Card,
  Col,
  DatePicker,
  Form,
  Grid,
  Image,
  Input,
  InputNumber,
  Modal,
  Popconfirm,
  Row,
  Select,
  Space,
  Table,
  Typography,
  message,
} from 'antd';
import ReactECharts from 'echarts-for-react';
import dayjs from 'dayjs';
import ResponsiveTableWrapper from '../components/common/ResponsiveTableWrapper';
import ResponsiveToolbar from '../components/common/ResponsiveToolbar';
import QCModuleTabs from '../components/QC/QCModuleTabs';
import { useAuth } from '../contexts/AuthContext';
import { useLang } from '../contexts/LangContext';
import {
  createProductionOutput,
  deleteProductionOutput,
  deleteQualityRecord,
  getQualityRecordFilterOptions,
  getRangePpm,
  getRangeSummary,
  listProductionOutputs,
  listQualityRecords,
  updateProductionOutput,
} from '../api/qcSystem';
import { formatServerUtcDateTime } from '../utils/dateTime';
import { resolveQcPhotoUrl } from '../utils/qcMedia';

const { RangePicker } = DatePicker;
const MONTH_OPTIONS = Array.from({ length: 12 }, (_, index) => ({
  value: index + 1,
  label: `${index + 1}`,
}));
const PPM_DECIMALS = 4;

function defaultDateRange() {
  return [dayjs().startOf('year'), dayjs()];
}

function buildSectionTitle(label) {
  return <Typography.Text strong style={{ fontSize: 16 }}>{label}</Typography.Text>;
}

function formatPpmValue(value) {
  const numericValue = Number(value);
  return Number.isFinite(numericValue) ? numericValue.toFixed(PPM_DECIMALS) : '0.0000';
}

export default function QCDashboard() {
  const screens = Grid.useBreakpoint();
  const isMobile = !screens.md;
  const { canDeleteQCDashboardData } = useAuth();
  const { t } = useLang();
  const currentYear = dayjs().year();
  const currentMonth = dayjs().month() + 1;
  const [messageApi, contextHolder] = message.useMessage();
  const [productionForm] = Form.useForm();

  const [pageLoading, setPageLoading] = useState(false);
  const [historyLoading, setHistoryLoading] = useState(false);
  const [productionLoading, setProductionLoading] = useState(false);
  const [filterOptions, setFilterOptions] = useState({
    defect_types: [],
    line_codes: [],
    battery_models: [],
  });
  const [filters, setFilters] = useState({
    dateRange: defaultDateRange(),
    line_codes: [],
    battery_models: [],
    defect_type_ids: [],
  });
  const [historyFilters, setHistoryFilters] = useState({
    keyword: '',
    dateRange: [],
    sort_by: 'id',
    sort_order: 'desc',
  });
  const [productionFilters, setProductionFilters] = useState({
    year: currentYear,
    month: undefined,
    line_codes: [],
    keyword: '',
  });
  const [summaryRows, setSummaryRows] = useState([]);
  const [ppmRows, setPpmRows] = useState([]);
  const [historyRows, setHistoryRows] = useState([]);
  const [productionRows, setProductionRows] = useState([]);
  const [productionModalOpen, setProductionModalOpen] = useState(false);
  const [editingProduction, setEditingProduction] = useState(null);
  const initializedRef = useRef(false);

  const lineMap = useMemo(
    () => Object.fromEntries((filterOptions.line_codes || []).map((item) => [item.code, item])),
    [filterOptions.line_codes],
  );

  const availableBatteryModels = useMemo(() => {
    if (!filters.line_codes.length) {
      return filterOptions.battery_models || [];
    }

    const values = new Set();
    filters.line_codes.forEach((code) => {
      const info = lineMap[code];
      if (info?.battery_model) values.add(info.battery_model);
    });
    return Array.from(values).sort();
  }, [filterOptions.battery_models, filters.line_codes, lineMap]);

  useEffect(() => {
    setFilters((prev) => ({
      ...prev,
      battery_models: prev.battery_models.filter((value) => availableBatteryModels.includes(value)),
    }));
  }, [availableBatteryModels]);

  const loadFilterOptions = useCallback(async () => {
    const { data } = await getQualityRecordFilterOptions();
    setFilterOptions({
      defect_types: data.defect_types || [],
      line_codes: data.line_codes || [],
      battery_models: data.battery_models || [],
    });
  }, []);

  const loadCharts = useCallback(async () => {
    const [startDate, endDate] = filters.dateRange || [];
    if (!startDate || !endDate) {
      setSummaryRows([]);
      setPpmRows([]);
      return;
    }

    const [{ data: summary }, { data: ppm }] = await Promise.all([
      getRangeSummary({
        start_date: startDate.format('YYYY-MM-DD'),
        end_date: endDate.format('YYYY-MM-DD'),
        line_codes: filters.line_codes,
        battery_models: filters.battery_models,
        defect_type_ids: filters.defect_type_ids,
      }),
      getRangePpm({
        start_date: startDate.format('YYYY-MM-DD'),
        end_date: endDate.format('YYYY-MM-DD'),
        line_codes: filters.line_codes,
        battery_models: filters.battery_models,
      }),
    ]);

    setSummaryRows(summary || []);
    setPpmRows(ppm || []);
  }, [filters]);

  const loadHistory = useCallback(async () => {
    setHistoryLoading(true);
    try {
      const { data } = await listQualityRecords({
        start_date: historyFilters.dateRange?.[0]?.format('YYYY-MM-DD'),
        end_date: historyFilters.dateRange?.[1]?.format('YYYY-MM-DD'),
        line_codes: filters.line_codes,
        battery_models: filters.battery_models,
        defect_type_ids: filters.defect_type_ids,
        keyword: historyFilters.keyword,
        sort_by: historyFilters.sort_by,
        sort_order: historyFilters.sort_order,
        limit: 10000,
      });
      setHistoryRows(data || []);
    } catch (error) {
      messageApi.error(error.response?.data?.detail || t('qc.load_error'));
    } finally {
      setHistoryLoading(false);
    }
  }, [filters.battery_models, filters.defect_type_ids, filters.line_codes, historyFilters, messageApi, t]);

  const loadProductionOutputs = useCallback(async () => {
    setProductionLoading(true);
    try {
      const { data } = await listProductionOutputs({
        year: productionFilters.year,
        month: productionFilters.month,
        line_codes: productionFilters.line_codes,
        keyword: productionFilters.keyword,
        sort_by: 'year',
        sort_order: 'desc',
        limit: 1000,
      });
      setProductionRows(data || []);
    } catch (error) {
      messageApi.error(error.response?.data?.detail || t('qc.load_error'));
    } finally {
      setProductionLoading(false);
    }
  }, [messageApi, productionFilters, t]);

  const loadAll = useCallback(async () => {
    setPageLoading(true);
    try {
      await Promise.all([loadCharts(), loadHistory(), loadProductionOutputs()]);
    } catch (error) {
      messageApi.error(error.response?.data?.detail || t('qc.load_error'));
    } finally {
      setPageLoading(false);
    }
  }, [loadCharts, loadHistory, loadProductionOutputs, messageApi, t]);

  useEffect(() => {
    if (initializedRef.current) return;
    initializedRef.current = true;

    Promise.all([loadFilterOptions(), loadAll()]).catch((error) => {
      messageApi.error(error.response?.data?.detail || t('qc.load_error'));
    });
  }, [loadAll, loadFilterOptions, messageApi, t]);

  const trendOption = useMemo(() => {
    const yearMonths = [...new Set(summaryRows.map((item) => item.year_month))].sort();
    const defectNames = [...new Set(summaryRows.map((item) => item.defect_name))].sort();

    return {
      tooltip: { trigger: 'axis' },
      legend: { type: 'scroll', bottom: 0 },
      grid: { left: 40, right: 16, top: 24, bottom: 56 },
      xAxis: { type: 'category', data: yearMonths, axisLabel: { rotate: 30 } },
      yAxis: { type: 'value', minInterval: 1 },
      series: defectNames.length
        ? defectNames.map((name) => ({
            name,
            type: 'line',
            smooth: true,
            data: yearMonths.map((yearMonth) => {
              const row = summaryRows.find((item) => item.year_month === yearMonth && item.defect_name === name);
              return row ? Number(row.count || 0) : 0;
            }),
          }))
        : [{ type: 'line', data: [] }],
    };
  }, [summaryRows]);

  const pieOption = useMemo(() => {
    const defectNames = [...new Set(summaryRows.map((item) => item.defect_name))].sort();
    const pieData = defectNames
      .map((name) => ({
        name,
        value: summaryRows
          .filter((item) => item.defect_name === name)
          .reduce((sum, item) => sum + Number(item.count || 0), 0),
      }))
      .filter((item) => item.value > 0);

    return {
      tooltip: { trigger: 'item', formatter: '{b}: {c} ({d}%)' },
      legend: { type: 'scroll', bottom: 0 },
      series: [
        {
          type: 'pie',
          radius: ['38%', '64%'],
          center: ['50%', '46%'],
          data: pieData,
          label: { formatter: '{b}\n{c}' },
        },
      ],
    };
  }, [summaryRows]);

  const ppmOption = useMemo(() => {
    const yearMonths = [...new Set(ppmRows.map((item) => item.year_month))].sort();
    const seriesMap = {};

    ppmRows.forEach((item) => {
      const key = item.battery_model ? `${item.line_code}-${item.battery_model}` : item.line_code;
      if (!seriesMap[key]) seriesMap[key] = {};
      seriesMap[key][item.year_month] = Number(item.ppm || 0);
    });

    return {
      tooltip: {
        trigger: 'axis',
        formatter: (params) => {
          const rows = Array.isArray(params) ? params : [params];
          const axisValue = rows[0]?.axisValueLabel || rows[0]?.name || '';
          return [
            axisValue,
            ...rows.map((item) => `${item.marker}${item.seriesName}: ${formatPpmValue(item.value)}`),
          ].join('<br/>');
        },
      },
      legend: { type: 'scroll', bottom: 0 },
      grid: { left: 48, right: 16, top: 24, bottom: 56 },
      xAxis: { type: 'category', data: yearMonths, axisLabel: { rotate: 30 } },
      yAxis: {
        type: 'value',
        name: 'PPM',
        axisLabel: { formatter: (value) => formatPpmValue(value) },
      },
      series: Object.keys(seriesMap).length
        ? Object.keys(seriesMap).map((key) => ({
            name: key,
            type: 'line',
            smooth: true,
            data: yearMonths.map((yearMonth) => seriesMap[key][yearMonth] ?? 0),
          }))
        : [{ type: 'line', data: [] }],
    };
  }, [ppmRows]);

  const ppmNeedsAttention = useMemo(() => {
    const totalDefects = summaryRows.reduce((sum, item) => sum + Number(item.count || 0), 0);
    const totalOutputs = ppmRows.reduce((sum, item) => sum + Number(item.output_qty || 0), 0);
    const matchedDefects = ppmRows.reduce((sum, item) => sum + Number(item.defect_count || 0), 0);
    return totalDefects > 0 && totalOutputs > 0 && matchedDefects === 0;
  }, [ppmRows, summaryRows]);

  const handleDeleteHistory = async (id) => {
    try {
      await deleteQualityRecord(id);
      messageApi.success(t('qc.delete_success'));
      await Promise.all([loadHistory(), loadCharts()]);
    } catch (error) {
      messageApi.error(error.response?.data?.detail || t('qc.delete_error'));
    }
  };

  const historyColumns = useMemo(
    () => [
      { title: t('qc.id_field'), dataIndex: 'id', width: 80 },
      { title: t('qc.detected_date'), dataIndex: 'detected_date', width: 120 },
      { title: t('qc.upper_code'), dataIndex: 'upper_code', width: 150 },
      { title: t('qc.lower_code'), dataIndex: 'lower_code', width: 110 },
      { title: t('qc.found_department'), dataIndex: 'found_department', width: 140 },
      { title: t('qc.ocv'), dataIndex: 'ocv', width: 110 },
      { title: t('qc.building_no'), dataIndex: 'building_no', width: 110 },
      { title: t('qc.line_code'), dataIndex: 'parsed_line_code', width: 90 },
      { title: t('qc.battery_model'), dataIndex: 'parsed_battery_model', width: 110 },
      { title: t('qc.station'), dataIndex: 'parsed_station_no', width: 90 },
      { title: t('qc.grade'), dataIndex: 'parsed_grade', width: 110 },
      { title: t('qc.defect_type'), dataIndex: 'defect_type_name', width: 120 },
      { title: t('qc.defect_description'), dataIndex: 'defect_description', width: 220 },
      { title: t('qc.operator_name'), dataIndex: 'operator_name', width: 110 },
      {
        title: t('qc.upload_photo'),
        dataIndex: 'photo_url',
        width: 90,
        render: (value) => {
          const resolvedValue = resolveQcPhotoUrl(value);
          return resolvedValue ? (
            <Image
              src={resolvedValue}
              width={48}
              height={48}
              style={{ borderRadius: 4, objectFit: 'cover' }}
              preview={{ src: resolvedValue }}
            />
          ) : (
            '-'
          );
        },
      },
      {
        title: t('qc.record_time'),
        dataIndex: 'record_time',
        width: 170,
        render: (value) => formatServerUtcDateTime(value),
      },
      {
        title: t('qc.actions'),
        key: 'actions',
        width: 100,
        fixed: 'right',
        render: (_, record) => (
          canDeleteQCDashboardData ? (
            <Popconfirm title={t('qc.delete_confirm')} onConfirm={() => handleDeleteHistory(record.id)}>
              <Button danger type="link">
                {t('qc.delete')}
              </Button>
            </Popconfirm>
          ) : null
        ),
      },
    ],
    [canDeleteQCDashboardData, t],
  );

  const openProductionDialog = (record = null) => {
    setEditingProduction(record);
    productionForm.setFieldsValue({
      year: record?.year ?? currentYear,
      month: record?.month ?? currentMonth,
      line_code: record?.line_code ?? undefined,
      line_desc: record?.line_desc ?? '',
      battery_model: record?.battery_model ?? '',
      output_qty: record?.output_qty ?? 0,
      note: record?.note ?? '',
    });
    setProductionModalOpen(true);
  };

  const handleProductionLineChange = (lineCode) => {
    const info = lineMap[lineCode];
    productionForm.setFieldsValue({
      line_desc: info?.label || '',
      battery_model: info?.battery_model || '',
    });
  };

  const handleSaveProduction = async () => {
    try {
      const values = await productionForm.validateFields();
      const payload = {
        year: Number(values.year),
        month: Number(values.month),
        line_code: values.line_code,
        line_desc: values.line_desc || null,
        battery_model: values.battery_model || null,
        output_qty: Number(values.output_qty || 0),
        note: values.note || null,
      };

      if (editingProduction?.id) {
        await updateProductionOutput(editingProduction.id, payload);
      } else {
        const { data: existingRows } = await listProductionOutputs({
          year: payload.year,
          month: payload.month,
          line_codes: [payload.line_code],
          limit: 10,
        });
        const matched = (existingRows || []).find(
          (item) =>
            Number(item.year) === payload.year &&
            Number(item.month) === payload.month &&
            String(item.line_code || '').toUpperCase() === String(payload.line_code || '').toUpperCase(),
        );

        if (matched?.id) {
          await updateProductionOutput(matched.id, payload);
          messageApi.info(t('qc.output_updated_existing'));
        } else {
          await createProductionOutput(payload);
        }
      }

      messageApi.success(t('qc.save_success'));
      setProductionModalOpen(false);
      setEditingProduction(null);
      await Promise.all([loadProductionOutputs(), loadCharts()]);
    } catch (error) {
      if (error?.errorFields) return;
      messageApi.error(error.response?.data?.detail || t('qc.save_error'));
    }
  };

  const handleDeleteProduction = async (id) => {
    try {
      await deleteProductionOutput(id);
      messageApi.success(t('qc.delete_success'));
      await Promise.all([loadProductionOutputs(), loadCharts()]);
    } catch (error) {
      messageApi.error(error.response?.data?.detail || t('qc.delete_error'));
    }
  };

  const productionColumns = useMemo(
    () => [
      { title: t('qc.production_year'), dataIndex: 'year', width: 90 },
      { title: t('qc.production_month'), dataIndex: 'month', width: 90 },
      { title: t('qc.line_code'), dataIndex: 'line_code', width: 110 },
      { title: t('qc.line_desc'), dataIndex: 'line_desc', width: 150 },
      { title: t('qc.battery_model'), dataIndex: 'battery_model', width: 130 },
      { title: t('qc.output_qty'), dataIndex: 'output_qty', width: 120 },
      { title: t('qc.note'), dataIndex: 'note', width: 160 },
      {
        title: t('qc.updated_at'),
        dataIndex: 'updated_at',
        width: 170,
        render: (value) => formatServerUtcDateTime(value),
      },
      {
        title: t('qc.actions'),
        key: 'actions',
        width: 140,
        fixed: 'right',
        render: (_, record) => (
          <Space size={0} direction="vertical">
            <Button type="link" onClick={() => openProductionDialog(record)}>
              {t('qc.edit')}
            </Button>
            {canDeleteQCDashboardData ? (
              <Popconfirm title={t('qc.delete_confirm')} onConfirm={() => handleDeleteProduction(record.id)}>
                <Button danger type="link">
                  {t('qc.delete')}
                </Button>
              </Popconfirm>
            ) : null}
          </Space>
        ),
      },
    ],
    [canDeleteQCDashboardData, t],
  );

  return (
    <div style={{ padding: isMobile ? '4px 0 20px' : '8px 16px 24px', background: '#f5f7fa', minHeight: 'calc(100vh - 96px)' }}>
      {contextHolder}
      <div style={{ background: '#fff', borderRadius: 8, padding: isMobile ? '0 16px 12px' : '0 24px 16px', marginBottom: 16 }}>
        <Typography.Title level={3} style={{ margin: 0, paddingTop: 20 }}>
          {t('qc.module_name')}
        </Typography.Title>
        <QCModuleTabs />
      </div>

      <Space direction="vertical" size={16} style={{ display: 'flex' }}>
        <Card title={buildSectionTitle(t('qc.filter_conditions'))} loading={pageLoading}>
          <Row gutter={[16, 16]} align="bottom">
            <Col xs={24} xl={7}>
              <Typography.Text>{t('qc.date_range')}</Typography.Text>
              <RangePicker
                value={filters.dateRange}
                style={{ width: '100%', marginTop: 8 }}
                onChange={(value) => setFilters((prev) => ({ ...prev, dateRange: value || [] }))}
              />
            </Col>
            <Col xs={24} xl={5}>
              <Typography.Text>{t('qc.line_multi')}</Typography.Text>
              <Select
                mode="multiple"
                allowClear
                value={filters.line_codes}
                style={{ width: '100%', marginTop: 8 }}
                options={filterOptions.line_codes.map((item) => ({
                  value: item.code,
                  label: `${item.code} - ${item.label}`,
                }))}
                onChange={(value) => setFilters((prev) => ({ ...prev, line_codes: value }))}
              />
            </Col>
            <Col xs={24} xl={5}>
              <Typography.Text>{t('qc.model_multi')}</Typography.Text>
              <Select
                mode="multiple"
                allowClear
                value={filters.battery_models}
                style={{ width: '100%', marginTop: 8 }}
                options={availableBatteryModels.map((item) => ({ value: item, label: item }))}
                onChange={(value) => setFilters((prev) => ({ ...prev, battery_models: value }))}
              />
            </Col>
            <Col xs={24} xl={5}>
              <Typography.Text>{t('qc.defect_multi')}</Typography.Text>
              <Select
                mode="multiple"
                allowClear
                value={filters.defect_type_ids}
                style={{ width: '100%', marginTop: 8 }}
                options={filterOptions.defect_types.map((item) => ({ value: item.id, label: item.name }))}
                onChange={(value) => setFilters((prev) => ({ ...prev, defect_type_ids: value }))}
              />
            </Col>
            <Col xs={24} xl={2}>
              <Button type="primary" style={{ width: '100%' }} onClick={loadAll}>
                {t('qc.refresh')}
              </Button>
            </Col>
          </Row>
        </Card>

        <Row gutter={[16, 16]}>
          <Col xs={24} xl={10}>
            <Card title={buildSectionTitle(t('qc.trend_title'))}>
              <ReactECharts option={trendOption} style={{ height: isMobile ? 260 : 300 }} notMerge />
            </Card>
          </Col>
          <Col xs={24} md={12} xl={7}>
            <Card title={buildSectionTitle(t('qc.pie_title'))}>
              <ReactECharts option={pieOption} style={{ height: isMobile ? 260 : 300 }} notMerge />
            </Card>
          </Col>
          <Col xs={24} md={12} xl={7}>
            <Card title={buildSectionTitle(t('qc.ppm_title'))}>
              {ppmNeedsAttention ? (
                <Alert
                  type="warning"
                  showIcon
                  style={{ marginBottom: 12 }}
                  message={t('qc.ppm_mismatch_warning')}
                />
              ) : null}
              <ReactECharts option={ppmOption} style={{ height: isMobile ? 260 : 300 }} notMerge />
            </Card>
          </Col>
        </Row>

        <Card title={buildSectionTitle(t('qc.history_title'))}>
          <Row gutter={[12, 12]} style={{ marginBottom: 12 }}>
            <Col xs={24} xl={6}>
              <Input
                value={historyFilters.keyword}
                placeholder={t('qc.history_search')}
                onChange={(event) => setHistoryFilters((prev) => ({ ...prev, keyword: event.target.value }))}
              />
            </Col>
            <Col xs={24} xl={6}>
              <RangePicker
                value={historyFilters.dateRange}
                style={{ width: '100%' }}
                onChange={(value) => setHistoryFilters((prev) => ({ ...prev, dateRange: value || [] }))}
              />
            </Col>
            <Col xs={24} md={8} xl={4}>
              <Select
                value={historyFilters.sort_by}
                style={{ width: '100%' }}
                options={[
                  { value: 'id', label: t('qc.sort_record_id') },
                  { value: 'record_time', label: t('qc.sort_record_time') },
                  { value: 'detected_date', label: t('qc.sort_detected_date') },
                  { value: 'parsed_production_time', label: t('qc.sort_production_time') },
                ]}
                onChange={(value) => setHistoryFilters((prev) => ({ ...prev, sort_by: value }))}
              />
            </Col>
            <Col xs={24} md={8} xl={3}>
              <Select
                value={historyFilters.sort_order}
                style={{ width: '100%' }}
                options={[
                  { value: 'desc', label: t('qc.desc') },
                  { value: 'asc', label: t('qc.asc') },
                ]}
                onChange={(value) => setHistoryFilters((prev) => ({ ...prev, sort_order: value }))}
              />
            </Col>
            <Col xs={24} md={8} xl={2}>
              <Button type="primary" style={{ width: '100%' }} onClick={loadHistory}>
                {t('qc.query')}
              </Button>
            </Col>
          </Row>

          <ResponsiveTableWrapper minWidth={1980}>
            <Table
              rowKey="id"
              size={isMobile ? 'small' : 'middle'}
              loading={historyLoading}
              dataSource={historyRows}
              columns={historyColumns}
              pagination={{ pageSize: 10, showSizeChanger: false }}
              scroll={{ x: 1980 }}
              locale={{ emptyText: t('qc.no_data') }}
            />
          </ResponsiveTableWrapper>
        </Card>

        <Card title={buildSectionTitle(t('qc.production_title'))}>
          <ResponsiveToolbar style={{ marginBottom: 12 }}>
            <div />
            <Button type="primary" onClick={() => openProductionDialog()} style={{ width: isMobile ? '100%' : undefined }}>
              {t('qc.add_output')}
            </Button>
          </ResponsiveToolbar>
          <Row gutter={[12, 12]} style={{ marginBottom: 12 }}>
            <Col xs={24} md={8} xl={4}>
              <InputNumber
                value={productionFilters.year}
                min={2000}
                max={2100}
                style={{ width: '100%' }}
                onChange={(value) => setProductionFilters((prev) => ({ ...prev, year: value || currentYear }))}
              />
            </Col>
            <Col xs={24} md={8} xl={4}>
              <Select
                allowClear
                value={productionFilters.month}
                placeholder={t('qc.month_placeholder')}
                style={{ width: '100%' }}
                options={MONTH_OPTIONS.map((item) => ({
                  value: item.value,
                  label: `${item.label}${t('qc.month_suffix')}`,
                }))}
                onChange={(value) => setProductionFilters((prev) => ({ ...prev, month: value }))}
              />
            </Col>
            <Col xs={24} md={8} xl={6}>
              <Select
                mode="multiple"
                allowClear
                value={productionFilters.line_codes}
                placeholder={t('qc.line_required')}
                style={{ width: '100%' }}
                options={filterOptions.line_codes.map((item) => ({
                  value: item.code,
                  label: `${item.code} - ${item.label}`,
                }))}
                onChange={(value) => setProductionFilters((prev) => ({ ...prev, line_codes: value }))}
              />
            </Col>
            <Col xs={24} xl={8}>
              <Input
                value={productionFilters.keyword}
                placeholder={t('qc.search_production')}
                onChange={(event) => setProductionFilters((prev) => ({ ...prev, keyword: event.target.value }))}
              />
            </Col>
            <Col xs={24} xl={2}>
              <Button type="primary" style={{ width: '100%' }} onClick={loadProductionOutputs}>
                {t('qc.query')}
              </Button>
            </Col>
          </Row>

          <ResponsiveTableWrapper minWidth={1200}>
            <Table
              rowKey="id"
              size={isMobile ? 'small' : 'middle'}
              loading={productionLoading}
              dataSource={productionRows}
              columns={productionColumns}
              pagination={{ pageSize: 10, showSizeChanger: false }}
              scroll={{ x: 1200 }}
              locale={{ emptyText: t('qc.no_data') }}
            />
          </ResponsiveTableWrapper>
        </Card>
      </Space>

      <Modal
        open={productionModalOpen}
        title={editingProduction ? t('qc.edit_output') : t('qc.add_output')}
        onCancel={() => {
          setProductionModalOpen(false);
          setEditingProduction(null);
        }}
        onOk={handleSaveProduction}
        okText={t('qc.save')}
        cancelText={t('qc.cancel')}
        destroyOnClose
      >
        <Form form={productionForm} layout="vertical">
          <Form.Item name="year" label={t('qc.production_year')} rules={[{ required: true }]}>
            <InputNumber min={2000} max={2100} style={{ width: '100%' }} />
          </Form.Item>
          <Form.Item name="month" label={t('qc.production_month')} rules={[{ required: true }]}>
            <InputNumber min={1} max={12} style={{ width: '100%' }} />
          </Form.Item>
          <Form.Item name="line_code" label={t('qc.line_required')} rules={[{ required: true, message: t('qc.line_required_msg') }]}>
            <Select
              showSearch
              options={filterOptions.line_codes.map((item) => ({
                value: item.code,
                label: `${item.code} - ${item.label}`,
              }))}
              placeholder={t('qc.select_line')}
              onChange={handleProductionLineChange}
            />
          </Form.Item>
          <Form.Item name="line_desc" label={t('qc.line_desc')}>
            <Input readOnly placeholder={t('qc.auto_fill')} />
          </Form.Item>
          <Form.Item name="battery_model" label={t('qc.battery_model')}>
            <Input readOnly placeholder={t('qc.auto_fill')} />
          </Form.Item>
          <Form.Item name="output_qty" label={t('qc.output_qty')} rules={[{ required: true }]}>
            <InputNumber min={0} style={{ width: '100%' }} />
          </Form.Item>
          <Form.Item name="note" label={t('qc.note')}>
            <Input.TextArea rows={2} />
          </Form.Item>
        </Form>
      </Modal>
    </div>
  );
}
