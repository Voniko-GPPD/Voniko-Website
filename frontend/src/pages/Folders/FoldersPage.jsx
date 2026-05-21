import React, { useState, useEffect, useCallback } from 'react';
import {
  Card, Button, Typography, Space, Row, Col, List, Tag,
  Modal, Form, Input, message, Popconfirm, Tooltip, Empty, Upload,
} from 'antd';
import {
  PlusOutlined, EditOutlined, DeleteOutlined, FolderOutlined,
  DesktopOutlined, DownloadOutlined, UploadOutlined, BankOutlined,
} from '@ant-design/icons';
import { useLang } from '../../contexts/LangContext';
import api, { getFolders, createFolder, updateFolder, deleteFolder } from '../../api';

const { Title, Text } = Typography;

export default function FoldersPage() {
  const { t } = useLang();
  const [workshops, setWorkshops] = useState([]);
  const [lines, setLines] = useState([]); // standalone lines (no workshop parent)
  const [loading, setLoading] = useState(false);
  const [selectedWorkshop, setSelectedWorkshop] = useState(null);
  const [selectedLine, setSelectedLine] = useState(null);

  const [modalOpen, setModalOpen] = useState(false);
  const [modalType, setModalType] = useState(null); // 'workshop' | 'line' | 'machine' | 'editWorkshop' | 'editLine' | 'editMachine'
  const [editTarget, setEditTarget] = useState(null);
  const [form] = Form.useForm();
  const [saving, setSaving] = useState(false);

  const fetchFolders = useCallback(async () => {
    setLoading(true);
    try {
      const res = await getFolders();
      const wsData = res.data.workshops || [];
      const linesData = res.data.lines || [];
      setWorkshops(wsData);
      setLines(linesData);
      if (selectedWorkshop) {
        const updated = wsData.find(w => w.id === selectedWorkshop.id);
        setSelectedWorkshop(updated || null);
        if (selectedLine) {
          const updatedLine = (updated?.lines || []).find(l => l.id === selectedLine.id);
          setSelectedLine(updatedLine || null);
        }
      } else if (selectedLine) {
        const updatedLine = linesData.find(l => l.id === selectedLine.id);
        setSelectedLine(updatedLine || null);
      }
    } catch {
      message.error(t('error'));
    } finally {
      setLoading(false);
    }
  }, [selectedWorkshop?.id, selectedLine?.id, t]);

  useEffect(() => { fetchFolders(); }, []);

  const openAddWorkshop = () => {
    setModalType('workshop');
    setEditTarget(null);
    form.resetFields();
    setModalOpen(true);
  };

  const openAddLine = () => {
    setModalType('line');
    setEditTarget(null);
    form.resetFields();
    setModalOpen(true);
  };

  const openAddMachine = () => {
    setModalType('machine');
    setEditTarget(null);
    form.resetFields();
    setModalOpen(true);
  };

  const openEdit = (folder, type) => {
    const editType = type === 'workshop' ? 'editWorkshop' : type === 'line' ? 'editLine' : 'editMachine';
    setModalType(editType);
    setEditTarget(folder);
    form.setFieldsValue({ name: folder.name, description: folder.description });
    setModalOpen(true);
  };

  const handleSave = async (values) => {
    setSaving(true);
    try {
      if (modalType === 'workshop') {
        await createFolder({ name: values.name, type: 'workshop', description: values.description });
        message.success(t('folderCreated'));
      } else if (modalType === 'line') {
        await createFolder({
          name: values.name,
          type: 'line',
          parentId: selectedWorkshop?.id || null,
          description: values.description,
        });
        message.success(t('folderCreated'));
      } else if (modalType === 'machine') {
        await createFolder({ name: values.name, type: 'machine', parentId: selectedLine.id, description: values.description });
        message.success(t('folderCreated'));
      } else if (modalType === 'editWorkshop' || modalType === 'editLine' || modalType === 'editMachine') {
        await updateFolder(editTarget.id, { name: values.name, description: values.description });
        message.success(t('folderUpdated'));
      }
      setModalOpen(false);
      form.resetFields();
      await fetchFolders();
    } catch (err) {
      message.error(err.response?.data?.message || t('error'));
    } finally {
      setSaving(false);
    }
  };

  const handleDelete = async (folder) => {
    try {
      await deleteFolder(folder.id);
      message.success(t('folderDeleted'));
      if (selectedWorkshop?.id === folder.id) { setSelectedWorkshop(null); setSelectedLine(null); }
      if (selectedLine?.id === folder.id) setSelectedLine(null);
      await fetchFolders();
    } catch (err) {
      message.error(err.response?.data?.message || t('folderHasFiles'));
    }
  };

  const handleExport = async () => {
    const token = localStorage.getItem('accessToken');
    const res = await fetch('/api/folders/export', { headers: { Authorization: `Bearer ${token}` } });
    const blob = await res.blob();
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'folders.xlsx';
    a.click();
    URL.revokeObjectURL(url);
  };

  const handleImport = async ({ file, onSuccess, onError }) => {
    const formData = new FormData();
    formData.append('file', file);
    try {
      const res = await api.post('/folders/import', formData, { headers: { 'Content-Type': 'multipart/form-data' } });
      message.success(res.data.message);
      onSuccess();
      fetchFolders();
    } catch (err) {
      message.error(err.response?.data?.message || t('error'));
      onError(err);
    }
  };

  const modalTitle = {
    workshop: t('addWorkshop'),
    line: t('addLine'),
    machine: t('addMachine'),
    editWorkshop: t('edit') + ' ' + t('workshop'),
    editLine: t('edit') + ' ' + t('line'),
    editMachine: t('edit') + ' ' + t('machine'),
  }[modalType];

  // Lines to show in the middle column
  const displayLines = selectedWorkshop ? (selectedWorkshop.lines || []) : lines;

  return (
    <div>
      <Title level={3} style={{ marginBottom: 16 }}>{t('folders')}</Title>

      <Space style={{ marginBottom: 16 }}>
        <Button icon={<DownloadOutlined />} onClick={handleExport}>{t('folderExport')}</Button>
        <Upload accept=".csv,.xlsx" showUploadList={false} customRequest={handleImport}>
          <Button icon={<UploadOutlined />}>{t('folderImport')}</Button>
        </Upload>
      </Space>

      <Row gutter={16}>
        {/* Col 1: Workshops */}
        <Col xs={24} md={8}>
          <Card
            title={
              <Space>
                <BankOutlined />
                <span>{t('workshops')}</span>
              </Space>
            }
            extra={
              <Button type="primary" size="small" icon={<PlusOutlined />} onClick={openAddWorkshop}>
                {t('addWorkshop')}
              </Button>
            }
            loading={loading}
          >
            {workshops.length === 0 ? (
              <Empty description={t('none')} />
            ) : (
              <List
                dataSource={workshops}
                renderItem={(ws) => (
                  <List.Item
                    key={ws.id}
                    style={{
                      cursor: 'pointer',
                      background: selectedWorkshop?.id === ws.id ? '#e6f4ff' : undefined,
                      borderRadius: 6,
                      padding: '8px 12px',
                    }}
                    onClick={() => { setSelectedWorkshop(ws); setSelectedLine(null); }}
                    actions={[
                      <Tooltip title={t('edit')} key="edit">
                        <Button
                          type="text"
                          size="small"
                          icon={<EditOutlined />}
                          onClick={(e) => { e.stopPropagation(); openEdit(ws, 'workshop'); }}
                        />
                      </Tooltip>,
                      <Popconfirm
                        key="delete"
                        title={t('deleteFileConfirm')}
                        onConfirm={(e) => { e?.stopPropagation(); handleDelete(ws); }}
                        okText={t('yes')}
                        cancelText={t('no')}
                      >
                        <Tooltip title={t('delete')}>
                          <Button
                            type="text"
                            size="small"
                            icon={<DeleteOutlined />}
                            danger
                            onClick={(e) => e.stopPropagation()}
                          />
                        </Tooltip>
                      </Popconfirm>,
                    ]}
                  >
                    <List.Item.Meta
                      title={<Text strong>{ws.name}</Text>}
                      description={
                        <Space size={4}>
                          <Tag color="purple">{ws.lines?.length || 0} {t('lines')}</Tag>
                          <Tag color="cyan">{ws.fileCount} files</Tag>
                        </Space>
                      }
                    />
                  </List.Item>
                )}
              />
            )}
          </Card>
        </Col>

        {/* Col 2: Lines within selected workshop (or standalone lines) */}
        <Col xs={24} md={8}>
          <Card
            title={
              <Space>
                <FolderOutlined />
                <span>
                  {selectedWorkshop ? `${t('lines')} — ${selectedWorkshop.name}` : t('lines')}
                </span>
              </Space>
            }
            extra={
              <Button type="primary" size="small" icon={<PlusOutlined />} onClick={openAddLine}>
                {t('addLine')}
              </Button>
            }
            loading={loading}
          >
            {displayLines.length === 0 ? (
              <Empty description={t('none')} />
            ) : (
              <List
                dataSource={displayLines}
                renderItem={(line) => (
                  <List.Item
                    key={line.id}
                    style={{
                      cursor: 'pointer',
                      background: selectedLine?.id === line.id ? '#e6f4ff' : undefined,
                      borderRadius: 6,
                      padding: '8px 12px',
                    }}
                    onClick={() => setSelectedLine(line)}
                    actions={[
                      <Tooltip title={t('edit')} key="edit">
                        <Button
                          type="text"
                          size="small"
                          icon={<EditOutlined />}
                          onClick={(e) => { e.stopPropagation(); openEdit(line, 'line'); }}
                        />
                      </Tooltip>,
                      <Popconfirm
                        key="delete"
                        title={t('deleteFileConfirm')}
                        onConfirm={(e) => { e?.stopPropagation(); handleDelete(line); }}
                        okText={t('yes')}
                        cancelText={t('no')}
                      >
                        <Tooltip title={t('delete')}>
                          <Button
                            type="text"
                            size="small"
                            icon={<DeleteOutlined />}
                            danger
                            onClick={(e) => e.stopPropagation()}
                          />
                        </Tooltip>
                      </Popconfirm>,
                    ]}
                  >
                    <List.Item.Meta
                      title={<Text strong>{line.name}</Text>}
                      description={
                        <Space size={4}>
                          <Tag color="blue">{line.machines?.length || 0} {t('machines')}</Tag>
                          <Tag color="cyan">{line.fileCount} files</Tag>
                        </Space>
                      }
                    />
                  </List.Item>
                )}
              />
            )}
          </Card>
        </Col>

        {/* Col 3: Machines within selected line */}
        <Col xs={24} md={8}>
          <Card
            title={
              <Space>
                <DesktopOutlined />
                <span>
                  {selectedLine ? `${t('machines')} — ${selectedLine.name}` : t('machines')}
                </span>
              </Space>
            }
            extra={
              selectedLine && (
                <Button type="primary" size="small" icon={<PlusOutlined />} onClick={openAddMachine}>
                  {t('addMachine')}
                </Button>
              )
            }
          >
            {!selectedLine ? (
              <Empty description={t('selectLine')} />
            ) : (selectedLine.machines || []).length === 0 ? (
              <Empty description={t('none')} />
            ) : (
              <List
                dataSource={selectedLine.machines}
                renderItem={(machine) => (
                  <List.Item
                    key={machine.id}
                    actions={[
                      <Tooltip title={t('edit')} key="edit">
                        <Button
                          type="text"
                          size="small"
                          icon={<EditOutlined />}
                          onClick={() => openEdit(machine, 'machine')}
                        />
                      </Tooltip>,
                      <Popconfirm
                        key="delete"
                        title={t('deleteFileConfirm')}
                        onConfirm={() => handleDelete(machine)}
                        okText={t('yes')}
                        cancelText={t('no')}
                      >
                        <Tooltip title={t('delete')}>
                          <Button type="text" size="small" icon={<DeleteOutlined />} danger />
                        </Tooltip>
                      </Popconfirm>,
                    ]}
                  >
                    <List.Item.Meta
                      title={<Text>{machine.name}</Text>}
                      description={
                        <Space size={4}>
                          <Tag color="cyan">{machine.fileCount} files</Tag>
                          {machine.description && <Text type="secondary">{machine.description}</Text>}
                        </Space>
                      }
                    />
                  </List.Item>
                )}
              />
            )}
          </Card>
        </Col>
      </Row>

      {/* Add/Edit Modal */}
      <Modal
        title={modalTitle}
        open={modalOpen}
        onCancel={() => { setModalOpen(false); form.resetFields(); }}
        footer={null}
        width={400}
      >
        <Form form={form} layout="vertical" onFinish={handleSave}>
          <Form.Item
            name="name"
            label={t('folderName')}
            rules={[{ required: true, message: t('folderName') + ' ' + t('error') }]}
          >
            <Input />
          </Form.Item>
          <Form.Item name="description" label={`${t('description')} ${t('optional')}`}>
            <Input.TextArea rows={2} />
          </Form.Item>
          <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 8 }}>
            <Button onClick={() => { setModalOpen(false); form.resetFields(); }}>{t('cancel')}</Button>
            <Button type="primary" htmlType="submit" loading={saving}>{t('save')}</Button>
          </div>
        </Form>
      </Modal>
    </div>
  );
}
