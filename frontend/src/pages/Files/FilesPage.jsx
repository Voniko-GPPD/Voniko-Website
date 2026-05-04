import React, { useState, useEffect, useCallback, useRef } from 'react';
import {
  Table, Button, Space, Input, Typography, Tag, Tooltip,
  Modal, Form, Upload, Select, message, Popconfirm, Card, Tabs,
  Row, Col, List, Empty,
} from 'antd';
import {
  UploadOutlined, PlusOutlined, SearchOutlined,
  DeleteOutlined, EyeOutlined, InboxOutlined, FolderOutlined, LockOutlined, TagOutlined, SyncOutlined,
  EditOutlined, DownloadOutlined, BankOutlined, DesktopOutlined,
} from '@ant-design/icons';
import { useNavigate } from 'react-router-dom';
import dayjs from 'dayjs';
import api, { getFolders, createFolder, updateFolder, deleteFolder } from '../../api';
import { useLang } from '../../contexts/LangContext';
import { useAuth } from '../../contexts/AuthContext';
import { removePendingUpload } from '../../utils/pendingUploads';

const { Title, Text } = Typography;
const { Dragger } = Upload;
const { Option } = Select;

function formatBytes(bytes) {
  if (!bytes && bytes !== 0) return '-';
  if (bytes === 0) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  return `${(bytes / Math.pow(1024, i)).toFixed(2)} ${units[i]}`;
}

export default function FilesPage() {
  const [files, setFiles] = useState([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(false);
  const [page, setPage] = useState(1);
  const [search, setSearch] = useState('');
  const [filterWorkshopId, setFilterWorkshopId] = useState(null);
  const [filterLineId, setFilterLineId] = useState(null);
  const [filterMachineId, setFilterMachineId] = useState(null);
  const [uploadModal, setUploadModal] = useState(false);
  const [uploadLoading, setUploadLoading] = useState(false);
  const [uploadFileId, setUploadFileId] = useState(null);
  const [uploadFileName, setUploadFileName] = useState(null);
  const [form] = Form.useForm();
  const [fileList, setFileList] = useState([]);
  const [folders, setFolders] = useState({ workshops: [], lines: [] });
  const foldersRef = useRef({ workshops: [], lines: [] });
  const [uploadWorkshopId, setUploadWorkshopId] = useState(null);
  const [uploadLineId, setUploadLineId] = useState(null);
  const [uploadMachineId, setUploadMachineId] = useState(null);
  const [allTags, setAllTags] = useState([]);
  const [filterTagId, setFilterTagId] = useState(null);

  // Rename state
  const [renameModal, setRenameModal] = useState(false);
  const [renameTarget, setRenameTarget] = useState(null);
  const [renameLoading, setRenameLoading] = useState(false);
  const [renameForm] = Form.useForm();

  // Folder management state
  const [fmWorkshops, setFmWorkshops] = useState([]);
  const [fmLines, setFmLines] = useState([]);
  const [fmLoading, setFmLoading] = useState(false);
  const [fmSelectedWorkshop, setFmSelectedWorkshop] = useState(null);
  const [fmSelectedLine, setFmSelectedLine] = useState(null);
  const [fmModalOpen, setFmModalOpen] = useState(false);
  const [fmModalType, setFmModalType] = useState(null);
  const [fmEditTarget, setFmEditTarget] = useState(null);
  const [fmForm] = Form.useForm();
  const [fmSaving, setFmSaving] = useState(false);

  const { t } = useLang();
  const { user: currentUser, canEdit, isAdmin, isEngineer } = useAuth();
  const navigate = useNavigate();

  const fetchFolders = useCallback(async () => {
    try {
      const res = await getFolders();
      foldersRef.current = res.data;
      setFolders(res.data);
    } catch {
      // silently ignore
    }
  }, []);

  const fetchTags = useCallback(async () => {
    try {
      const res = await api.get('/tags');
      setAllTags(res.data);
    } catch { /* ignore */ }
  }, []);

  const fetchFiles = useCallback(async () => {
    setLoading(true);
    try {
      const params = { page, limit: 20 };
      if (search) params.search = search;
      if (filterMachineId) params.folderId = filterMachineId;
      const res = await api.get('/files', { params });
      let data = res.data.data;
      // client-side filter by line when no specific machine is selected
      if (filterLineId && !filterMachineId) {
        const allLines = [
          ...(foldersRef.current.workshops || []).flatMap(w => w.lines || []),
          ...(foldersRef.current.lines || []),
        ];
        const line = allLines.find(l => l.id === filterLineId);
        if (line) {
          const machineIds = new Set((line.machines || []).map(m => m.id));
          data = data.filter(f => f.folderId && machineIds.has(f.folderId));
        }
      } else if (filterWorkshopId && !filterLineId && !filterMachineId) {
        const workshop = (foldersRef.current.workshops || []).find(w => w.id === filterWorkshopId);
        if (workshop) {
          const machineIds = new Set(
            (workshop.lines || []).flatMap(l => (l.machines || []).map(m => m.id))
          );
          data = data.filter(f => f.folderId && machineIds.has(f.folderId));
        }
      }
      // client-side filter by tag
      if (filterTagId) {
        data = data.filter(f => (f.tags || []).some(tag => tag.id === filterTagId));
      }
      setFiles(data);
      setTotal((filterLineId && !filterMachineId) || (filterWorkshopId && !filterLineId && !filterMachineId) ? data.length : res.data.total);
    } finally {
      setLoading(false);
    }
  }, [page, search, filterWorkshopId, filterLineId, filterMachineId, filterTagId]);

  // Folder management fetch
  const fetchFmFolders = useCallback(async () => {
    setFmLoading(true);
    try {
      const res = await getFolders();
      const wsData = res.data.workshops || [];
      const linesData = res.data.lines || [];
      setFmWorkshops(wsData);
      setFmLines(linesData);
      // refresh selected items
      setFmSelectedWorkshop(prev => {
        if (!prev) return prev;
        const updated = wsData.find(w => w.id === prev.id);
        return updated || null;
      });
      setFmSelectedLine(prev => {
        if (!prev) return prev;
        // find in workshop lines or standalone
        const allLines = [...wsData.flatMap(w => w.lines || []), ...linesData];
        return allLines.find(l => l.id === prev.id) || null;
      });
    } catch {
      message.error(t('error'));
    } finally {
      setFmLoading(false);
    }
  }, [t]);

  useEffect(() => { fetchFolders(); fetchTags(); fetchFmFolders(); }, []);
  useEffect(() => { fetchFiles(); }, [fetchFiles]);

  const handleSearch = (value) => { setSearch(value); setPage(1); };
  const handleFilterWorkshop = (value) => { setFilterWorkshopId(value || null); setFilterLineId(null); setFilterMachineId(null); setPage(1); };
  const handleFilterLine = (value) => { setFilterLineId(value || null); setFilterMachineId(null); setPage(1); };
  const handleFilterMachine = (value) => { setFilterMachineId(value || null); setPage(1); };
  const handleFilterTag = (value) => { setFilterTagId(value || null); setPage(1); };

  const handleUpload = async (values) => {
    if (!fileList.length) { message.error(t('selectFile')); return; }
    setUploadLoading(true);
    try {
      const formData = new FormData();
      formData.append('file', fileList[0].originFileObj);
      if (values.customFileName && values.customFileName.trim()) {
        const custom = values.customFileName.trim();
        const origName = fileList[0].name;
        const origDotIdx = origName.lastIndexOf('.');
        const originalExt = origDotIdx > 0 ? origName.slice(origDotIdx) : '';
        const customDotIdx = custom.lastIndexOf('.');
        const customExt = customDotIdx > 0 ? custom.slice(customDotIdx) : '';
        const finalCustomName = (!customExt && originalExt) ? custom + originalExt : custom;
        formData.append('customFileName', finalCustomName);
      }
      if (values.commitMessage) formData.append('commitMessage', values.commitMessage);
      if (values.description) formData.append('description', values.description);
      // When uploading a new version for a specific file, send fileId for stable identification
      if (uploadFileId) {
        formData.append('fileId', uploadFileId);
      } else if (uploadMachineId) {
        formData.append('folderId', uploadMachineId);
      }
      await api.post('/files', formData, { headers: { 'Content-Type': 'multipart/form-data' } });
      if (uploadFileId) removePendingUpload(uploadFileId);
      message.success(t('fileUploaded'));
      setUploadModal(false);
      form.resetFields();
      setFileList([]);
      setUploadWorkshopId(null);
      setUploadLineId(null);
      setUploadMachineId(null);
      setUploadFileId(null);
      setUploadFileName(null);
      fetchFiles();
    } catch (err) {
      message.error(err.response?.data?.message || t('error'));
    } finally {
      setUploadLoading(false);
    }
  };

  const handleDelete = async (id) => {
    try {
      await api.delete(`/files/${id}`);
      message.success(t('fileDeleted'));
      fetchFiles();
    } catch (err) {
      message.error(err.response?.data?.message || t('error'));
    }
  };

  const openRename = (record) => {
    setRenameTarget(record);
    renameForm.setFieldsValue({ name: record.name });
    setRenameModal(true);
  };

  const handleRename = async (values) => {
    setRenameLoading(true);
    try {
      const trimmedName = values.name.trim();
      const oldDotIdx = renameTarget.name.lastIndexOf('.');
      const oldExt = oldDotIdx > 0 ? renameTarget.name.slice(oldDotIdx) : '';
      const newDotIdx = trimmedName.lastIndexOf('.');
      const newExt = newDotIdx > 0 ? trimmedName.slice(newDotIdx) : '';
      const finalName = (!newExt && oldExt) ? trimmedName + oldExt : trimmedName;
      await api.patch(`/files/${renameTarget.id}`, { name: finalName });
      message.success(t('renameSuccess'));
      setRenameModal(false);
      renameForm.resetFields();
      setRenameTarget(null);
      fetchFiles();
    } catch (err) {
      message.error(err.response?.data?.message || t('error'));
    } finally {
      setRenameLoading(false);
    }
  };

  const allFilterLines = filterWorkshopId
    ? ((folders.workshops || []).find(w => w.id === filterWorkshopId)?.lines || [])
    : [
        ...(folders.workshops || []).flatMap(w => w.lines || []),
        ...(folders.lines || []),
      ];
  const filterLine = allFilterLines.find(l => l.id === filterLineId);
  const filterMachines = filterLine ? (filterLine.machines || []) : [];
  const uploadLines = uploadWorkshopId
    ? ((folders.workshops || []).find(w => w.id === uploadWorkshopId)?.lines || [])
    : [
        ...(folders.workshops || []).flatMap(w => w.lines || []),
        ...(folders.lines || []),
      ];
  const uploadLine = uploadLines.find(l => l.id === uploadLineId);
  const uploadMachines = uploadLine ? (uploadLine.machines || []) : [];

  const closeModal = () => {
    setUploadModal(false);
    form.resetFields();
    setFileList([]);
    setUploadWorkshopId(null);
    setUploadLineId(null);
    setUploadMachineId(null);
    setUploadFileId(null);
    setUploadFileName(null);
  };

  const canDeleteFile = (record) => {
    if (isAdmin) return true;
    if (isEngineer && record.createdById === currentUser?.id) return true;
    return false;
  };

  const columns = [
    {
      title: t('fileName'),
      dataIndex: 'name',
      render: (name, record) => (
        <div>
          <Space size={4}>
            <Button type="link" style={{ padding: 0 }} onClick={() => navigate(`/files/${record.id}`)}>
              {name}
            </Button>
            {record.lockedBy && (
              <Tooltip title={`${t('fileLockedBy')}: ${record.lockedByName || record.lockedBy}`}>
                <LockOutlined style={{ color: '#faad14', fontSize: 13 }} />
              </Tooltip>
            )}
          </Space>
          {(record.tags || []).length > 0 && (
            <div style={{ marginTop: 2 }}>
              {record.tags.map(tag => <Tag key={tag.id} color={tag.color} style={{ marginBottom: 2, fontSize: 11 }}>{tag.name}</Tag>)}
            </div>
          )}
        </div>
      ),
    },
    {
      title: t('folderPath'),
      dataIndex: 'folderPath',
      render: (fp, record) => {
        if (fp) {
          return (
            <Space size={4}>
              <FolderOutlined style={{ color: '#1677ff' }} />
              <Text type="secondary">{fp}</Text>
            </Space>
          );
        }
        if (record.path && record.path !== '/') {
          return <Text type="secondary">{record.path}</Text>;
        }
        return <Text type="secondary">—</Text>;
      },
    },
    {
      title: t('versions'),
      dataIndex: 'versionCount',
      width: 90,
      render: (v) => <Tag color="blue">{v}</Tag>,
    },
    {
      title: t('fileSize'),
      dataIndex: 'currentSize',
      width: 100,
      render: formatBytes,
    },
    {
      title: t('updatedAt'),
      dataIndex: 'lastModified',
      width: 160,
      render: (v) => v ? dayjs(v).format('YYYY-MM-DD HH:mm') : '-',
    },
    {
      title: t('createdBy'),
      dataIndex: 'createdBy',
      width: 120,
    },
    {
      title: t('actions'),
      width: 130,
      render: (_, record) => (
        <Space>
          <Tooltip title={t('viewDetails')}>
            <Button type="text" size="small" icon={<EyeOutlined />} onClick={() => navigate(`/files/${record.id}`)} />
          </Tooltip>
          {(canEdit || isAdmin) && (
            <Tooltip title={t('renameFile')}>
              <Button type="text" size="small" icon={<EditOutlined />} onClick={() => openRename(record)} />
            </Tooltip>
          )}
          {(canEdit || isAdmin) && (
            <Tooltip title={t('uploadNewVersion')}>
              <Button
                type="text"
                size="small"
                icon={<SyncOutlined />}
                onClick={() => {
                  setUploadFileId(record.id);
                  setUploadFileName(record.name);
                  setUploadModal(true);
                }}
              />
            </Tooltip>
          )}
          {canDeleteFile(record) && (
            <Popconfirm
              title={t('deleteFileConfirm')}
              onConfirm={() => handleDelete(record.id)}
              okText={t('yes')}
              cancelText={t('no')}
            >
              <Tooltip title={t('deleteFile')}>
                <Button type="text" size="small" icon={<DeleteOutlined />} danger />
              </Tooltip>
            </Popconfirm>
          )}
        </Space>
      ),
    },
  ];

  // ---- Folder management helpers ----
  const fmOpenAddWorkshop = () => {
    setFmModalType('workshop'); setFmEditTarget(null); fmForm.resetFields(); setFmModalOpen(true);
  };
  const fmOpenAddLine = () => {
    setFmModalType('line'); setFmEditTarget(null); fmForm.resetFields(); setFmModalOpen(true);
  };
  const fmOpenAddMachine = () => {
    setFmModalType('machine'); setFmEditTarget(null); fmForm.resetFields(); setFmModalOpen(true);
  };
  const fmOpenEdit = (folder, type) => {
    const editType = type === 'workshop' ? 'editWorkshop' : type === 'line' ? 'editLine' : 'editMachine';
    setFmModalType(editType); setFmEditTarget(folder);
    fmForm.setFieldsValue({ name: folder.name, description: folder.description });
    setFmModalOpen(true);
  };
  const fmHandleSave = async (values) => {
    setFmSaving(true);
    try {
      if (fmModalType === 'workshop') {
        await createFolder({ name: values.name, type: 'workshop', description: values.description });
        message.success(t('folderCreated'));
      } else if (fmModalType === 'line') {
        await createFolder({ name: values.name, type: 'line', parentId: fmSelectedWorkshop?.id || null, description: values.description });
        message.success(t('folderCreated'));
      } else if (fmModalType === 'machine') {
        await createFolder({ name: values.name, type: 'machine', parentId: fmSelectedLine.id, description: values.description });
        message.success(t('folderCreated'));
      } else {
        await updateFolder(fmEditTarget.id, { name: values.name, description: values.description });
        message.success(t('folderUpdated'));
      }
      setFmModalOpen(false);
      fmForm.resetFields();
      await fetchFmFolders();
      await fetchFolders();
    } catch (err) {
      message.error(err.response?.data?.message || t('error'));
    } finally {
      setFmSaving(false);
    }
  };
  const fmHandleDelete = async (folder) => {
    try {
      await deleteFolder(folder.id);
      message.success(t('folderDeleted'));
      if (fmSelectedWorkshop?.id === folder.id) { setFmSelectedWorkshop(null); setFmSelectedLine(null); }
      if (fmSelectedLine?.id === folder.id) setFmSelectedLine(null);
      await fetchFmFolders();
      await fetchFolders();
    } catch (err) {
      message.error(err.response?.data?.message || t('folderHasFiles'));
    }
  };
  const fmHandleExport = async () => {
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
  const fmHandleImport = async ({ file, onSuccess, onError }) => {
    const formData = new FormData();
    formData.append('file', file);
    try {
      const res = await api.post('/folders/import', formData, { headers: { 'Content-Type': 'multipart/form-data' } });
      message.success(res.data.message);
      onSuccess();
      fetchFmFolders();
      fetchFolders();
    } catch (err) {
      message.error(err.response?.data?.message || t('error'));
      onError(err);
    }
  };

  const fmModalTitle = {
    workshop: t('addWorkshop'),
    line: t('addLine'),
    machine: t('addMachine'),
    editWorkshop: t('edit') + ' ' + t('workshop'),
    editLine: t('edit') + ' ' + t('line'),
    editMachine: t('edit') + ' ' + t('machine'),
  }[fmModalType];

  const fmDisplayLines = fmSelectedWorkshop ? (fmSelectedWorkshop.lines || []) : fmLines;

  // Tab items
  const tabItems = [
    {
      key: 'files',
      label: t('fileList'),
      children: (
        <Card>
          <div style={{ marginBottom: 16, display: 'flex', flexWrap: 'wrap', gap: 8 }}>
            <Input.Search
              placeholder={t('search')}
              allowClear
              onSearch={handleSearch}
              style={{ maxWidth: 300 }}
              prefix={<SearchOutlined />}
            />
            {(folders.workshops || []).length > 0 && (
              <Select
                allowClear
                placeholder={t('allWorkshops')}
                style={{ width: 180 }}
                onChange={handleFilterWorkshop}
                value={filterWorkshopId}
              >
                {(folders.workshops || []).map(w => <Option key={w.id} value={w.id}>{w.name}</Option>)}
              </Select>
            )}
            <Select
              allowClear
              placeholder={t('allLines')}
              style={{ width: 180 }}
              onChange={handleFilterLine}
              value={filterLineId}
            >
              {allFilterLines.map(l => <Option key={l.id} value={l.id}>{l.name}</Option>)}
            </Select>
            {filterLineId && (
              <Select
                allowClear
                placeholder={t('allMachines')}
                style={{ width: 180 }}
                onChange={handleFilterMachine}
                value={filterMachineId}
              >
                {filterMachines.map(m => <Option key={m.id} value={m.id}>{m.name}</Option>)}
              </Select>
            )}
            {allTags.length > 0 && (
              <Select
                allowClear
                placeholder={t('filterByTag')}
                style={{ width: 160 }}
                onChange={handleFilterTag}
                value={filterTagId}
                suffixIcon={<TagOutlined />}
              >
                {allTags.map(tag => (
                  <Option key={tag.id} value={tag.id}>
                    <Space size={4}>
                      <span style={{ display: 'inline-block', width: 10, height: 10, borderRadius: '50%', background: tag.color }} />
                      {tag.name}
                    </Space>
                  </Option>
                ))}
              </Select>
            )}
          </div>

          <Table
            columns={columns}
            dataSource={files}
            rowKey="id"
            loading={loading}
            pagination={{
              current: page,
              total,
              pageSize: 20,
              onChange: (p) => setPage(p),
              showTotal: (tot) => `${tot} ${t('total')}`,
            }}
            size="middle"
          />
        </Card>
      ),
    },
    ...(isAdmin || isEngineer ? [{
      key: 'folders',
      label: t('folderManagement'),
      children: (
        <div>
          <Space style={{ marginBottom: 16 }}>
            <Button icon={<DownloadOutlined />} onClick={fmHandleExport}>{t('folderExport')}</Button>
            <Upload accept=".csv,.xlsx" showUploadList={false} customRequest={fmHandleImport}>
              <Button icon={<UploadOutlined />}>{t('folderImport')}</Button>
            </Upload>
          </Space>

          <Row gutter={16}>
            <Col xs={24} md={8}>
              <Card
                title={<Space><BankOutlined /><span>{t('workshops')}</span></Space>}
                extra={
                  <Button type="primary" size="small" icon={<PlusOutlined />} onClick={fmOpenAddWorkshop}>
                    {t('addWorkshop')}
                  </Button>
                }
                loading={fmLoading}
              >
                {fmWorkshops.length === 0 ? (
                  <Empty description={t('none')} />
                ) : (
                  <List
                    dataSource={fmWorkshops}
                    renderItem={(ws) => (
                      <List.Item
                        key={ws.id}
                        style={{ cursor: 'pointer', background: fmSelectedWorkshop?.id === ws.id ? '#e6f4ff' : undefined, borderRadius: 6, padding: '8px 12px' }}
                        onClick={() => { setFmSelectedWorkshop(ws); setFmSelectedLine(null); }}
                        actions={[
                          <Tooltip title={t('edit')} key="edit">
                            <Button type="text" size="small" icon={<EditOutlined />} onClick={(e) => { e.stopPropagation(); fmOpenEdit(ws, 'workshop'); }} />
                          </Tooltip>,
                          <Popconfirm key="delete" title={t('deleteFileConfirm')} onConfirm={(e) => { e?.stopPropagation(); fmHandleDelete(ws); }} okText={t('yes')} cancelText={t('no')}>
                            <Tooltip title={t('delete')}><Button type="text" size="small" icon={<DeleteOutlined />} danger onClick={(e) => e.stopPropagation()} /></Tooltip>
                          </Popconfirm>,
                        ]}
                      >
                        <List.Item.Meta
                          title={<Text strong>{ws.name}</Text>}
                          description={<Space size={4}><Tag color="purple">{ws.lines?.length || 0} {t('lines')}</Tag><Tag color="cyan">{ws.fileCount} files</Tag></Space>}
                        />
                      </List.Item>
                    )}
                  />
                )}
              </Card>
            </Col>

            <Col xs={24} md={8}>
              <Card
                title={<Space><FolderOutlined /><span>{fmSelectedWorkshop ? `${t('lines')} — ${fmSelectedWorkshop.name}` : t('lines')}</span></Space>}
                extra={<Button type="primary" size="small" icon={<PlusOutlined />} onClick={fmOpenAddLine}>{t('addLine')}</Button>}
                loading={fmLoading}
              >
                {fmDisplayLines.length === 0 ? (
                  <Empty description={t('none')} />
                ) : (
                  <List
                    dataSource={fmDisplayLines}
                    renderItem={(line) => (
                      <List.Item
                        key={line.id}
                        style={{ cursor: 'pointer', background: fmSelectedLine?.id === line.id ? '#e6f4ff' : undefined, borderRadius: 6, padding: '8px 12px' }}
                        onClick={() => setFmSelectedLine(line)}
                        actions={[
                          <Tooltip title={t('edit')} key="edit">
                            <Button type="text" size="small" icon={<EditOutlined />} onClick={(e) => { e.stopPropagation(); fmOpenEdit(line, 'line'); }} />
                          </Tooltip>,
                          <Popconfirm key="delete" title={t('deleteFileConfirm')} onConfirm={(e) => { e?.stopPropagation(); fmHandleDelete(line); }} okText={t('yes')} cancelText={t('no')}>
                            <Tooltip title={t('delete')}><Button type="text" size="small" icon={<DeleteOutlined />} danger onClick={(e) => e.stopPropagation()} /></Tooltip>
                          </Popconfirm>,
                        ]}
                      >
                        <List.Item.Meta
                          title={<Text strong>{line.name}</Text>}
                          description={<Space size={4}><Tag color="blue">{line.machines?.length || 0} {t('machines')}</Tag><Tag color="cyan">{line.fileCount} files</Tag></Space>}
                        />
                      </List.Item>
                    )}
                  />
                )}
              </Card>
            </Col>

            <Col xs={24} md={8}>
              <Card
                title={<Space><DesktopOutlined /><span>{fmSelectedLine ? `${t('machines')} — ${fmSelectedLine.name}` : t('machines')}</span></Space>}
                extra={fmSelectedLine && (
                  <Button type="primary" size="small" icon={<PlusOutlined />} onClick={fmOpenAddMachine}>{t('addMachine')}</Button>
                )}
              >
                {!fmSelectedLine ? (
                  <Empty description={t('selectLine')} />
                ) : (fmSelectedLine.machines || []).length === 0 ? (
                  <Empty description={t('none')} />
                ) : (
                  <List
                    dataSource={fmSelectedLine.machines}
                    renderItem={(machine) => (
                      <List.Item
                        key={machine.id}
                        actions={[
                          <Tooltip title={t('edit')} key="edit">
                            <Button type="text" size="small" icon={<EditOutlined />} onClick={() => fmOpenEdit(machine, 'machine')} />
                          </Tooltip>,
                          <Popconfirm key="delete" title={t('deleteFileConfirm')} onConfirm={() => fmHandleDelete(machine)} okText={t('yes')} cancelText={t('no')}>
                            <Tooltip title={t('delete')}><Button type="text" size="small" icon={<DeleteOutlined />} danger /></Tooltip>
                          </Popconfirm>,
                        ]}
                      >
                        <List.Item.Meta
                          title={<Text>{machine.name}</Text>}
                          description={<Space size={4}><Tag color="cyan">{machine.fileCount} files</Tag>{machine.description && <Text type="secondary">{machine.description}</Text>}</Space>}
                        />
                      </List.Item>
                    )}
                  />
                )}
              </Card>
            </Col>
          </Row>

          <Modal
            title={fmModalTitle}
            open={fmModalOpen}
            onCancel={() => { setFmModalOpen(false); fmForm.resetFields(); }}
            footer={null}
            width={400}
          >
            <Form form={fmForm} layout="vertical" onFinish={fmHandleSave}>
              <Form.Item name="name" label={t('folderName')} rules={[{ required: true, message: t('folderName') + ' ' + t('error') }]}>
                <Input />
              </Form.Item>
              <Form.Item name="description" label={`${t('description')} ${t('optional')}`}>
                <Input.TextArea rows={2} />
              </Form.Item>
              <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 8 }}>
                <Button onClick={() => { setFmModalOpen(false); fmForm.resetFields(); }}>{t('cancel')}</Button>
                <Button type="primary" htmlType="submit" loading={fmSaving}>{t('save')}</Button>
              </div>
            </Form>
          </Modal>
        </div>
      ),
    }] : []),
  ];

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
        <Title level={3} style={{ margin: 0 }}>{t('fileList')}</Title>
        {canEdit && (
          <Button type="primary" icon={<PlusOutlined />} onClick={() => setUploadModal(true)}>
            {t('uploadFile')}
          </Button>
        )}
      </div>

      <Tabs items={tabItems} />

      <Modal
        title={uploadFileId ? t('uploadNewVersion') : t('uploadFile')}
        open={uploadModal}
        onCancel={closeModal}
        footer={null}
        width={560}
      >
        <Form form={form} layout="vertical" onFinish={handleUpload}>
          {uploadFileId && (
            <Form.Item label={t('fileName')}>
              <Input value={uploadFileName} readOnly />
            </Form.Item>
          )}
          <Form.Item label={t('selectFile')} required>
            <Dragger
              fileList={fileList}
              beforeUpload={() => false}
              onChange={({ fileList: fl }) => {
                setFileList(fl.slice(-1));
                if (fl.length > 0 && !uploadFileId) {
                  const name = fl[fl.length - 1].name;
                  form.setFieldValue('customFileName', name);
                }
              }}
              maxCount={1}
            >
              <p className="ant-upload-drag-icon"><InboxOutlined /></p>
              <p className="ant-upload-text">{t('uploadHint')}</p>
              <p className="ant-upload-hint">{t('uploadHint2')}</p>
            </Dragger>
          </Form.Item>

          {!uploadFileId && (
            <Form.Item name="customFileName" label={t('customFileName')}>
              <Input placeholder={t('customFileName')} />
            </Form.Item>
          )}

          {!uploadFileId && (
            <>
              {(folders.workshops || []).length > 0 && (
                <Form.Item label={t('selectWorkshop')}>
                  <Select
                    allowClear
                    placeholder={t('selectWorkshop')}
                    onChange={(val) => { setUploadWorkshopId(val || null); setUploadLineId(null); setUploadMachineId(null); }}
                    value={uploadWorkshopId}
                  >
                    {(folders.workshops || []).map(w => <Option key={w.id} value={w.id}>{w.name}</Option>)}
                  </Select>
                </Form.Item>
              )}

              <Form.Item label={t('selectLine')}>
                <Select
                  allowClear
                  placeholder={t('selectLine')}
                  onChange={(val) => { setUploadLineId(val || null); setUploadMachineId(null); }}
                  value={uploadLineId}
                >
                  {uploadLines.map(l => <Option key={l.id} value={l.id}>{l.name}</Option>)}
                </Select>
              </Form.Item>

              {uploadLineId && (
                <Form.Item label={t('selectMachine')}>
                  <Select
                    allowClear
                    placeholder={t('selectMachine')}
                    onChange={(val) => setUploadMachineId(val || null)}
                    value={uploadMachineId}
                  >
                    {uploadMachines.map(m => <Option key={m.id} value={m.id}>{m.name}</Option>)}
                  </Select>
                </Form.Item>
              )}

              <Form.Item name="description" label={`${t('description')} ${t('optional')}`}>
                <Input.TextArea rows={2} placeholder={t('descriptionPlaceholder')} />
              </Form.Item>
            </>
          )}

          <Form.Item name="commitMessage" label={`${t('commitMessage')} ${t('optional')}`}>
            <Input.TextArea rows={2} placeholder={t('commitMessagePlaceholder')} />
          </Form.Item>

          <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 8 }}>
            <Button onClick={closeModal}>{t('cancel')}</Button>
            <Button type="primary" htmlType="submit" loading={uploadLoading} icon={<UploadOutlined />}>
              {t('upload')}
            </Button>
          </div>
        </Form>
      </Modal>

      <Modal
        title={t('renameFileTitle')}
        open={renameModal}
        onCancel={() => { setRenameModal(false); renameForm.resetFields(); setRenameTarget(null); }}
        footer={null}
        width={400}
      >
        <Form form={renameForm} layout="vertical" onFinish={handleRename}>
          <Form.Item
            name="name"
            label={t('newFileName')}
            rules={[{ required: true, message: t('newFileName') }]}
          >
            <Input />
          </Form.Item>
          <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 8 }}>
            <Button onClick={() => { setRenameModal(false); renameForm.resetFields(); setRenameTarget(null); }}>{t('cancel')}</Button>
            <Button type="primary" htmlType="submit" loading={renameLoading}>{t('save')}</Button>
          </div>
        </Form>
      </Modal>
    </div>
  );
}
