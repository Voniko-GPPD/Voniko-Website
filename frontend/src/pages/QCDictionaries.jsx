import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  Button,
  Card,
  Form,
  Grid,
  Input,
  InputNumber,
  Modal,
  Popconfirm,
  Select,
  Space,
  Switch,
  Table,
  Tag,
  Typography,
  message,
} from 'antd';
import ResponsiveTableWrapper from '../components/common/ResponsiveTableWrapper';
import ResponsiveToolbar from '../components/common/ResponsiveToolbar';
import QCModuleTabs from '../components/QC/QCModuleTabs';
import { useAuth } from '../contexts/AuthContext';
import { useLang } from '../contexts/LangContext';
import {
  createDictionary,
  deleteDictionary,
  exportDictionaryFile,
  importDictionaryFile,
  listDictionary,
  updateDictionary,
} from '../api/qcSystem';

const DICT_CONFIG = {
  'line-mappings': {
    labelKey: 'dict_line',
    fields: ['code', 'line_desc', 'battery_model', 'status'],
  },
  'defect-types': {
    labelKey: 'dict_defect',
    fields: ['name', 'status'],
  },
  'year-mappings': {
    labelKey: 'dict_year',
    fields: ['code', 'year_value', 'status'],
  },
  'month-mappings': {
    labelKey: 'dict_month',
    fields: ['code', 'month_value', 'status'],
  },
  'grade-mappings': {
    labelKey: 'dict_grade',
    fields: ['code', 'grade_desc', 'status'],
  },
  'suffix-mappings': {
    labelKey: 'dict_suffix',
    fields: ['suffix_code', 'status_desc', 'status'],
  },
};

export default function QCDictionaries() {
  const screens = Grid.useBreakpoint();
  const isMobile = !screens.md;
  const {
    canExportQCDictionaries,
    canImportQCDictionaries,
    canMutateQCDictionaries,
  } = useAuth();
  const { t } = useLang();
  const [messageApi, contextHolder] = message.useMessage();
  const [form] = Form.useForm();
  const fileInputRef = useRef(null);
  const [activeKey, setActiveKey] = useState('line-mappings');
  const [loading, setLoading] = useState(false);
  const [rows, setRows] = useState([]);
  const [modalOpen, setModalOpen] = useState(false);
  const [editingRecord, setEditingRecord] = useState(null);

  const currentConfig = DICT_CONFIG[activeKey];

  const fieldLabel = useCallback(
    (field) => {
      const map = {
        id: 'id_field',
        code: 'code_field',
        line_desc: 'line_desc_field',
        battery_model: 'battery_model_field',
        status: 'active_field',
        name: 'name_field',
        year_value: 'year_value_field',
        month_value: 'month_value_field',
        grade_desc: 'grade_desc_field',
        suffix_code: 'suffix_code_field',
        status_desc: 'status_desc_field',
      };
      return t(`qc.${map[field]}`);
    },
    [t],
  );

  const loadRows = useCallback(async () => {
    setLoading(true);
    try {
      const { data } = await listDictionary(activeKey);
      setRows(data || []);
    } catch (error) {
      messageApi.error(error.response?.data?.detail || t('qc.load_error'));
    } finally {
      setLoading(false);
    }
  }, [activeKey, messageApi, t]);

  useEffect(() => {
    loadRows();
  }, [loadRows]);

  const openCreate = () => {
    if (!canMutateQCDictionaries) return;
    setEditingRecord(null);
    const values = currentConfig.fields.reduce((acc, field) => {
      acc[field] = field === 'status' ? true : '';
      return acc;
    }, {});
    form.setFieldsValue(values);
    setModalOpen(true);
  };

  const openEdit = (record) => {
    if (!canMutateQCDictionaries) return;
    setEditingRecord(record);
    form.setFieldsValue(record);
    setModalOpen(true);
  };

  const handleSave = async () => {
    if (!canMutateQCDictionaries) return;
    try {
      const values = await form.validateFields();
      if (editingRecord?.id) {
        await updateDictionary(activeKey, editingRecord.id, values);
      } else {
        await createDictionary(activeKey, values);
      }
      messageApi.success(t('qc.save_success'));
      setModalOpen(false);
      setEditingRecord(null);
      await loadRows();
    } catch (error) {
      if (error?.errorFields) return;
      messageApi.error(error.response?.data?.detail || t('qc.save_error'));
    }
  };

  const handleDelete = async (id) => {
    if (!canMutateQCDictionaries) return;
    try {
      await deleteDictionary(activeKey, id);
      messageApi.success(t('qc.delete_success'));
      await loadRows();
    } catch (error) {
      messageApi.error(error.response?.data?.detail || t('qc.delete_error'));
    }
  };

  const handleExport = async () => {
    if (!canExportQCDictionaries) return;
    try {
      const response = await exportDictionaryFile(activeKey);
      const contentDisposition = response.headers?.['content-disposition'] || '';
      const match = contentDisposition.match(/filename\*=UTF-8''(.+)/i);
      const fileName = match ? decodeURIComponent(match[1]) : `${activeKey}.xlsx`;
      const url = URL.createObjectURL(new Blob([response.data]));
      const link = document.createElement('a');
      link.href = url;
      link.download = fileName;
      link.click();
      URL.revokeObjectURL(url);
      messageApi.success(t('qc.export_success'));
    } catch (error) {
      messageApi.error(error.response?.data?.detail || t('qc.export_error'));
    }
  };

  const handleImportFile = async (event) => {
    if (!canImportQCDictionaries) return;
    const file = event.target.files?.[0];
    if (!file) return;

    try {
      await importDictionaryFile(activeKey, file);
      messageApi.success(t('qc.import_success'));
      await loadRows();
    } catch (error) {
      const detail = error.response?.data?.detail;
      messageApi.error(typeof detail === 'string' ? detail : t('qc.import_error'));
    } finally {
      event.target.value = '';
    }
  };

  const columns = useMemo(
    () => [
      { title: fieldLabel('id'), dataIndex: 'id', width: 80, align: 'center' },
      ...currentConfig.fields.map((field) => ({
        title: fieldLabel(field),
        dataIndex: field,
        render: (value) =>
          field === 'status' ? (
            <Tag color={value ? 'success' : 'default'}>{value ? t('qc.enabled') : t('qc.disabled')}</Tag>
          ) : (
            value || '-'
          ),
      })),
      ...(canMutateQCDictionaries ? [{
        title: t('qc.actions'),
        key: 'actions',
        width: 180,
        align: 'center',
        render: (_, record) => (
          <Space>
            <Button size="small" onClick={() => openEdit(record)}>
              {t('qc.edit')}
            </Button>
            <Popconfirm title={t('qc.delete_confirm')} onConfirm={() => handleDelete(record.id)}>
              <Button size="small" danger>
                {t('qc.delete')}
              </Button>
            </Popconfirm>
          </Space>
        ),
      }] : []),
    ],
    [canMutateQCDictionaries, currentConfig.fields, fieldLabel, t],
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

      <Card title={<Typography.Text strong style={{ fontSize: 16 }}>{t('qc.dict_title')}</Typography.Text>}>
        <ResponsiveToolbar style={{ marginBottom: 16 }}>
          <Select
            value={activeKey}
            style={{ width: isMobile ? '100%' : 220 }}
            options={Object.keys(DICT_CONFIG).map((key) => ({
              value: key,
              label: t(`qc.${DICT_CONFIG[key].labelKey}`),
            }))}
            optionLabelProp="label"
            onChange={setActiveKey}
          />
          <Space wrap style={{ width: isMobile ? '100%' : 'auto', justifyContent: isMobile ? 'stretch' : 'flex-end' }}>
            {canMutateQCDictionaries ? (
              <Button type="primary" onClick={openCreate} style={{ width: isMobile ? '100%' : undefined }}>
                {t('qc.add_entry')}
              </Button>
            ) : null}
            {canExportQCDictionaries ? (
              <Button style={{ background: '#67c23a', borderColor: '#67c23a', color: '#fff', width: isMobile ? '100%' : undefined }} onClick={handleExport}>
                {t('qc.export_excel')}
              </Button>
            ) : null}
            {canImportQCDictionaries ? (
              <Button
                style={{ background: '#e6a23c', borderColor: '#e6a23c', color: '#fff', width: isMobile ? '100%' : undefined }}
                onClick={() => fileInputRef.current?.click()}
              >
                {t('qc.import_excel')}
              </Button>
            ) : null}
          </Space>
          {canImportQCDictionaries ? (
            <input
              ref={fileInputRef}
              type="file"
              accept=".xlsx,.xls,.csv"
              style={{ display: 'none' }}
              onChange={handleImportFile}
            />
          ) : null}
        </ResponsiveToolbar>

        <ResponsiveTableWrapper minWidth={900}>
          <Table
            rowKey="id"
            size={isMobile ? 'small' : 'middle'}
            loading={loading}
            dataSource={rows}
            columns={columns}
            pagination={{ pageSize: 10, showSizeChanger: false }}
            scroll={{ x: 900 }}
            locale={{ emptyText: t('qc.no_data') }}
          />
        </ResponsiveTableWrapper>
      </Card>

      {canMutateQCDictionaries ? (
        <Modal
          open={modalOpen}
          title={editingRecord ? t('qc.edit_entry') : t('qc.add_entry')}
          onCancel={() => {
            setModalOpen(false);
            setEditingRecord(null);
          }}
          onOk={handleSave}
          okText={t('qc.save')}
          cancelText={t('qc.cancel')}
        >
          <Form form={form} layout="vertical">
            {currentConfig.fields.map((field) => {
              const numeric = field === 'year_value' || field === 'month_value';
              const status = field === 'status';
              return (
                <Form.Item
                  key={field}
                  name={field}
                  label={fieldLabel(field)}
                  valuePropName={status ? 'checked' : 'value'}
                  rules={status ? [] : [{ required: field !== 'battery_model' }]}
                >
                  {status ? (
                    <Switch />
                  ) : numeric ? (
                    <InputNumber style={{ width: '100%' }} />
                  ) : (
                    <Input />
                  )}
                </Form.Item>
              );
            })}
          </Form>
        </Modal>
      ) : null}
    </div>
  );
}
