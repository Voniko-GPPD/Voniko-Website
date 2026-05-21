import React, { useCallback, useEffect, useMemo, useState } from 'react';
import {
  Badge,
  Button,
  Card,
  Col,
  DatePicker,
  Descriptions,
  Divider,
  Empty,
  Grid,
  Image,
  Input,
  Row,
  Select,
  Space,
  Tag,
  Typography,
  Upload,
  message,
} from 'antd';
import { CalendarOutlined, CameraOutlined, CheckCircleOutlined, InfoCircleOutlined, ScanOutlined, UploadOutlined } from '@ant-design/icons';
import dayjs from 'dayjs';
import QCModuleTabs from '../components/QC/QCModuleTabs';
import ResponsiveToolbar from '../components/common/ResponsiveToolbar';
import { useAuth } from '../contexts/AuthContext';
import { useLang } from '../contexts/LangContext';
import { createQualityRecordWithPhoto, listDictionary, parseCodes } from '../api/qcSystem';
import { resolveQcPhotoUrl } from '../utils/qcMedia';

function emptyForm(defaultOperator = '') {
  return {
    detected_date: dayjs(),
    upper_code: '',
    lower_code: '',
    defect_type_id: null,
    defect_description: '',
    operator_name: defaultOperator,
  };
}

function fieldBlock(label, children, helper = null) {
  return (
    <div>
      <Typography.Text strong style={{ display: 'block', marginBottom: 8 }}>
        {label}
      </Typography.Text>
      {children}
      {helper ? (
        <Typography.Text type="secondary" style={{ display: 'block', marginTop: 8, fontSize: 12 }}>
          {helper}
        </Typography.Text>
      ) : null}
    </div>
  );
}

export default function QCEntry() {
  const screens = Grid.useBreakpoint();
  const isMobile = !screens.md;
  const { t } = useLang();
  const { user } = useAuth();
  const defaultOperator = useMemo(() => user?.displayName || user?.username || '', [user]);
  const [messageApi, contextHolder] = message.useMessage();
  const [loading, setLoading] = useState(false);
  const [parsing, setParsing] = useState(false);
  const [defectTypes, setDefectTypes] = useState([]);
  const [parsedCard, setParsedCard] = useState(null);
  const [photoFile, setPhotoFile] = useState(null);
  const [form, setForm] = useState(() => emptyForm(defaultOperator));

  useEffect(() => {
    setForm((prev) => ({
      ...prev,
      operator_name: prev.operator_name || defaultOperator,
    }));
  }, [defaultOperator]);

  const loadDefectTypes = useCallback(async () => {
    const { data } = await listDictionary('defect-types', { enabled_only: true });
    setDefectTypes(data || []);
  }, []);

  useEffect(() => {
    loadDefectTypes().catch((error) => {
      messageApi.error(error.response?.data?.detail || t('qc.load_error'));
    });
  }, [loadDefectTypes, messageApi, t]);

  const activeDefect = useMemo(
    () => defectTypes.find((item) => item.id === form.defect_type_id) || null,
    [defectTypes, form.defect_type_id],
  );

  const handleParse = async () => {
    if (!form.upper_code || !form.lower_code) {
      messageApi.warning(t('qc.warn_fill_code'));
      return;
    }

    setParsing(true);
    try {
      const { data } = await parseCodes({
        upper_code: form.upper_code,
        lower_code: form.lower_code,
      });
      setParsedCard(data);
    } catch (error) {
      messageApi.error(error.response?.data?.detail || t('qc.parse_error'));
    } finally {
      setParsing(false);
    }
  };

  const handleSubmit = async () => {
    if (!form.upper_code || !form.lower_code || !form.defect_type_id || !form.operator_name) {
      messageApi.warning(t('qc.warn_fill_all'));
      return;
    }

    setLoading(true);
    try {
      const body = new FormData();
      body.append('detected_date', form.detected_date.format('YYYY-MM-DD'));
      body.append('upper_code', form.upper_code);
      body.append('lower_code', form.lower_code);
      body.append('defect_type_id', String(form.defect_type_id));
      body.append('operator_name', form.operator_name);
      if (form.defect_description) body.append('defect_description', form.defect_description);
      if (photoFile) body.append('photo', photoFile);

      const { data } = await createQualityRecordWithPhoto(body);
      setParsedCard(data);
      setForm(emptyForm(defaultOperator));
      setPhotoFile(null);
      messageApi.success(t('qc.submit_success'));
    } catch (error) {
      messageApi.error(error.response?.data?.detail || t('qc.save_error'));
    } finally {
      setLoading(false);
    }
  };

  const suffixTokens = parsedCard?.suffix_tokens || (parsedCard?.suffix ? [parsedCard.suffix] : []);

  return (
    <div style={{ padding: isMobile ? '4px 0 20px' : '8px 16px 24px', background: '#f5f7fa', minHeight: 'calc(100vh - 96px)' }}>
      {contextHolder}

      <div style={{ background: '#fff', borderRadius: 8, padding: isMobile ? '0 16px 12px' : '0 24px 16px', marginBottom: 16 }}>
        <Typography.Title level={3} style={{ margin: 0, paddingTop: 20 }}>
          {t('qc.module_name')}
        </Typography.Title>
        <QCModuleTabs />
      </div>

      <div style={{ maxWidth: 1320, margin: '0 auto' }}>
        <Row gutter={[16, 16]}>
          <Col xs={24} xl={15}>
            <Card
              styles={{ body: { padding: 24 } }}
              style={{ borderRadius: 8, border: '1px solid #eef2f6', boxShadow: '0 10px 28px rgba(15, 23, 42, 0.06)' }}
              title={
                <Space size={10}>
                  <ScanOutlined />
                  <span>{t('qc.entry_title')}</span>
                </Space>
              }
            >
              <Row gutter={[16, 20]}>
                <Col xs={24} md={8}>
                  {fieldBlock(
                    t('qc.detected_date'),
                    <DatePicker
                      value={form.detected_date}
                      size="large"
                      suffixIcon={<CalendarOutlined />}
                      style={{ width: '100%' }}
                      onChange={(value) => setForm((prev) => ({ ...prev, detected_date: value || dayjs() }))}
                    />,
                  )}
                </Col>
                <Col xs={24} md={8}>
                  {fieldBlock(
                    t('qc.upper_code'),
                    <Input
                      size="large"
                      value={form.upper_code}
                      placeholder={t('qc.upper_code_placeholder')}
                      onChange={(event) => setForm((prev) => ({ ...prev, upper_code: event.target.value }))}
                    />,
                  )}
                </Col>
                <Col xs={24} md={8}>
                  {fieldBlock(
                    t('qc.lower_code'),
                    <Input
                      size="large"
                      value={form.lower_code}
                      placeholder={t('qc.lower_code_placeholder')}
                      onChange={(event) => setForm((prev) => ({ ...prev, lower_code: event.target.value }))}
                    />,
                  )}
                </Col>

                <Col xs={24} md={8}>
                  {fieldBlock(
                    t('qc.operator_name'),
                    <Input
                      size="large"
                      value={form.operator_name}
                      placeholder={t('qc.operator_placeholder')}
                      onChange={(event) => setForm((prev) => ({ ...prev, operator_name: event.target.value }))}
                    />,
                  )}
                </Col>
                <Col xs={24} md={16}>
                  {fieldBlock(
                    t('qc.upload_photo_optional'),
                    <div
                      style={{
                        display: 'flex',
                        alignItems: 'center',
                        gap: 12,
                        minHeight: 48,
                        flexWrap: 'wrap',
                      }}
                    >
                      <Upload
                        beforeUpload={(file) => {
                          setPhotoFile(file);
                          return false;
                        }}
                        onRemove={() => setPhotoFile(null)}
                        fileList={photoFile ? [photoFile] : []}
                        maxCount={1}
                        accept="image/*"
                        showUploadList={false}
                      >
                        <Button size="large" icon={<UploadOutlined />}>
                          {t('qc.select_photo')}
                        </Button>
                      </Upload>
                      {photoFile ? (
                        <div
                          style={{
                            minWidth: 0,
                            flex: '1 1 260px',
                            border: '1px solid #e8eef5',
                            borderRadius: 8,
                            padding: '10px 12px',
                            display: 'flex',
                            alignItems: 'center',
                            gap: 10,
                            background: '#fafcff',
                          }}
                        >
                          <CameraOutlined style={{ fontSize: 18, color: '#1677ff', flexShrink: 0 }} />
                          <div style={{ minWidth: 0 }}>
                            <Typography.Text strong ellipsis style={{ display: 'block' }}>
                              {photoFile.name}
                            </Typography.Text>
                            <Typography.Text type="secondary" style={{ fontSize: 12 }}>
                              {Math.round(photoFile.size / 1024)} KB
                            </Typography.Text>
                          </div>
                        </div>
                      ) : null}
                    </div>,
                  )}
                </Col>
              </Row>

              <Divider style={{ margin: '24px 0 20px' }} />

              {fieldBlock(
                t('qc.defect_type_single'),
                <Select
                  size="large"
                  value={form.defect_type_id}
                  placeholder={t('qc.select_defect_type')}
                  style={{ width: '100%' }}
                  options={defectTypes.map((item) => ({ value: item.id, label: item.name }))}
                  onChange={(value) => setForm((prev) => ({ ...prev, defect_type_id: value }))}
                />,
              )}

              {activeDefect ? (
                <div
                  style={{
                    marginTop: 12,
                    padding: '10px 12px',
                    borderRadius: 8,
                    background: '#f6ffed',
                    border: '1px solid #b7eb8f',
                  }}
                >
                  <Typography.Text strong>{activeDefect.name}</Typography.Text>
                </div>
              ) : null}

              <div style={{ marginTop: 24 }}>
                <Typography.Text strong style={{ display: 'block', marginBottom: 8 }}>
                  {t('qc.defect_description')}
                </Typography.Text>
                <Input.TextArea
                  rows={4}
                  value={form.defect_description}
                  placeholder={t('qc.defect_description_placeholder')}
                  onChange={(event) => setForm((prev) => ({ ...prev, defect_description: event.target.value }))}
                />
              </div>

              <ResponsiveToolbar justify="flex-start" style={{ marginTop: 24 }}>
                <Button
                  type="primary"
                  size="large"
                  loading={loading}
                  onClick={handleSubmit}
                  style={{ minWidth: isMobile ? '100%' : 190, boxShadow: '0 8px 18px rgba(22, 119, 255, 0.24)' }}
                  icon={<CheckCircleOutlined />}
                >
                  {t('qc.submit_and_parse')}
                </Button>
                <Button size="large" loading={parsing} onClick={handleParse} style={{ minWidth: isMobile ? '100%' : 190 }}>
                  {t('qc.preview_parse')}
                </Button>
              </ResponsiveToolbar>
            </Card>
          </Col>

          <Col xs={24} xl={9}>
            <Card
              styles={{ body: { padding: 24 } }}
              style={{ borderRadius: 8, height: '100%', border: '1px solid #eef2f6', boxShadow: '0 10px 28px rgba(15, 23, 42, 0.06)' }}
              title={
                <Space size={10}>
                  <InfoCircleOutlined />
                  <span>{t('qc.parse_result')}</span>
                </Space>
              }
            >
              {parsedCard ? (
                <Space direction="vertical" size={16} style={{ width: '100%' }}>
                  <div
                    style={{
                      display: 'flex',
                      justifyContent: 'space-between',
                      alignItems: 'center',
                      padding: '12px 14px',
                      borderRadius: 8,
                      background: '#f6ffed',
                      border: '1px solid #b7eb8f',
                    }}
                  >
                    <div>
                      <Typography.Text strong>{parsedCard.parsed_line || parsedCard.production_line || '-'}</Typography.Text>
                      <Typography.Text type="secondary" style={{ display: 'block', fontSize: 12 }}>
                        {parsedCard.parsed_battery_model || parsedCard.battery_model || '-'}
                      </Typography.Text>
                    </div>
                    {activeDefect ? <Badge color="#1677ff" text={activeDefect.name} /> : null}
                  </div>

                  <Descriptions
                    column={1}
                    size="small"
                    bordered
                    labelStyle={{ width: '42%', fontWeight: 600 }}
                    contentStyle={{ background: '#fff' }}
                  >
                    <Descriptions.Item label={t('qc.product_line')}>
                      {parsedCard.parsed_line || parsedCard.production_line || '-'}
                    </Descriptions.Item>
                    <Descriptions.Item label={t('qc.battery_model')}>
                      {parsedCard.parsed_battery_model || parsedCard.battery_model || '-'}
                    </Descriptions.Item>
                    <Descriptions.Item label={t('qc.station')}>
                      {parsedCard.parsed_station_no || parsedCard.station_no || '-'}
                    </Descriptions.Item>
                    <Descriptions.Item label={t('qc.parsed_time')}>
                      {parsedCard.parsed_production_time || parsedCard.production_time || '-'}
                    </Descriptions.Item>
                    <Descriptions.Item label={t('qc.grade')}>
                      {parsedCard.parsed_grade || parsedCard.grade || '-'}
                    </Descriptions.Item>
                    <Descriptions.Item label={t('qc.special_status')}>
                      {parsedCard.parsed_special_status || parsedCard.special_status || '-'}
                    </Descriptions.Item>
                    <Descriptions.Item label={t('qc.suffix_tag')}>
                      {suffixTokens.length ? suffixTokens.map((token) => <Tag key={token}>{token}</Tag>) : '-'}
                    </Descriptions.Item>
                    <Descriptions.Item label={t('qc.site_photo')}>
                      {resolveQcPhotoUrl(parsedCard.photo_url) ? (
                        <Image
                          src={resolveQcPhotoUrl(parsedCard.photo_url)}
                          width={160}
                          height={160}
                          style={{ borderRadius: 8, objectFit: 'cover' }}
                        />
                      ) : (
                        '-'
                      )}
                    </Descriptions.Item>
                  </Descriptions>
                </Space>
              ) : (
                <div
                  style={{
                    minHeight: 420,
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center',
                  }}
                >
                  <Empty description={t('qc.parse_result')} image={Empty.PRESENTED_IMAGE_SIMPLE} />
                </div>
              )}
            </Card>
          </Col>
        </Row>
      </div>
    </div>
  );
}
