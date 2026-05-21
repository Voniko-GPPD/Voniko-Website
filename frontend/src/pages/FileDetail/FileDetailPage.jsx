import React, { useState, useEffect, useCallback } from 'react';
import {
  Card, Typography, Tag, Button, Space, Modal, Upload, Form, Input,
  message, Popconfirm, Row, Col, Descriptions, Divider, Spin, Tabs,
  Tooltip, Badge, Alert, Select, ColorPicker, Avatar, List, Image,
} from 'antd';
import {
  ArrowLeftOutlined, UploadOutlined, DownloadOutlined, SwapOutlined,
  RollbackOutlined, InboxOutlined, TagOutlined, FolderOutlined,
  LockOutlined, UnlockOutlined, ExpandOutlined, ShrinkOutlined,
  BellOutlined, BellFilled, CommentOutlined, PlusOutlined, EditOutlined,
  DeleteOutlined, EyeOutlined, UserOutlined, CheckOutlined,
} from '@ant-design/icons';
import { useParams, useNavigate } from 'react-router-dom';
import dayjs from 'dayjs';
import api from '../../api';
import { lockFile, unlockFile } from '../../api';
import { useLang } from '../../contexts/LangContext';
import { useAuth } from '../../contexts/AuthContext';
import CommitGraph from '../../components/CommitGraph/CommitGraph';
import FileDiff from '../../components/FileDiff/FileDiff';
import { addPendingUpload, removePendingUpload } from '../../utils/pendingUploads';

const { Title, Text } = Typography;
const { Dragger } = Upload;
const { TextArea } = Input;

const PRESET_COLORS = ['#f5222d','#fa541c','#fa8c16','#fadb14','#52c41a','#13c2c2','#1677ff','#722ed1','#eb2f96','#8c8c8c'];

function formatBytes(bytes) {
  if (!bytes && bytes !== 0) return '-';
  if (bytes === 0) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  return `${(bytes / Math.pow(1024, i)).toFixed(1)} ${units[i]}`;
}

function isImageMime(mime) {
  return mime && mime.startsWith('image/');
}

function isVideoMime(mime) {
  return mime && mime.startsWith('video/');
}

function isAudioMime(mime) {
  return mime && mime.startsWith('audio/');
}

function isPreviewableMime(mime) {
  return isImageMime(mime) || isVideoMime(mime) || isAudioMime(mime);
}

// Comment section for a version
function VersionComments({ versionId, user, isAdmin, canEdit, t }) {
  const [comments, setComments] = useState([]);
  const [loading, setLoading] = useState(false);
  const [newComment, setNewComment] = useState('');
  const [sending, setSending] = useState(false);
  const [editId, setEditId] = useState(null);
  const [editContent, setEditContent] = useState('');

  const fetchComments = useCallback(async () => {
    setLoading(true);
    try {
      const res = await api.get(`/versions/${versionId}/comments`);
      setComments(res.data);
    } catch { /* ignore */ }
    finally { setLoading(false); }
  }, [versionId]);

  useEffect(() => { fetchComments(); }, [fetchComments]);

  const handleSend = async () => {
    if (!newComment.trim()) { message.warning(t('commentEmpty')); return; }
    setSending(true);
    try {
      const res = await api.post(`/versions/${versionId}/comments`, { content: newComment.trim() });
      setComments(prev => [...prev, res.data]);
      setNewComment('');
    } catch (err) { message.error(err.response?.data?.message || t('error')); }
    finally { setSending(false); }
  };

  const handleDelete = async (commentId) => {
    try {
      await api.delete(`/versions/comments/${commentId}`);
      setComments(prev => prev.filter(c => c.id !== commentId));
      message.success(t('commentDeleted'));
    } catch (err) { message.error(err.response?.data?.message || t('error')); }
  };

  const handleEdit = async (commentId) => {
    if (!editContent.trim()) { message.warning(t('commentEmpty')); return; }
    try {
      await api.put(`/versions/comments/${commentId}`, { content: editContent.trim() });
      setComments(prev => prev.map(c => c.id === commentId ? { ...c, content: editContent.trim() } : c));
      setEditId(null);
      setEditContent('');
      message.success(t('commentUpdated'));
    } catch (err) { message.error(err.response?.data?.message || t('error')); }
  };

  return (
    <div style={{ marginTop: 8 }}>
      {loading ? <Spin size="small" /> : (
        comments.length === 0 ? (
          <Text type="secondary" style={{ fontSize: 12 }}>{t('noComments')}</Text>
        ) : (
          <List
            size="small"
            dataSource={comments}
            renderItem={c => (
              <List.Item
                style={{ padding: '6px 0', alignItems: 'flex-start' }}
                actions={
                  (c.userId === user?.id || isAdmin) ? [
                    <Button
                      key="edit"
                      type="text"
                      size="small"
                      icon={<EditOutlined />}
                      onClick={() => { setEditId(c.id); setEditContent(c.content); }}
                    />,
                    <Popconfirm
                      key="del"
                      title={t('deleteCommentConfirm')}
                      onConfirm={() => handleDelete(c.id)}
                      okText={t('yes')}
                      cancelText={t('no')}
                    >
                      <Button type="text" size="small" danger icon={<DeleteOutlined />} />
                    </Popconfirm>,
                  ] : []
                }
              >
                <List.Item.Meta
                  avatar={<Avatar icon={<UserOutlined />} size="small" style={{ background: '#1677ff' }} />}
                  title={<span style={{ fontSize: 12 }}><strong>{c.userName}</strong> <Text type="secondary" style={{ fontSize: 11 }}>{dayjs(c.createdAt).format('HH:mm DD/MM/YY')}</Text></span>}
                  description={
                    editId === c.id ? (
                      <Space.Compact style={{ width: '100%' }}>
                        <Input
                          size="small"
                          value={editContent}
                          onChange={e => setEditContent(e.target.value)}
                          onPressEnter={() => handleEdit(c.id)}
                        />
                        <Button size="small" type="primary" icon={<CheckOutlined />} onClick={() => handleEdit(c.id)} />
                        <Button size="small" onClick={() => { setEditId(null); setEditContent(''); }}>{t('cancel')}</Button>
                      </Space.Compact>
                    ) : (
                      <span style={{ fontSize: 12 }}>{c.content}</span>
                    )
                  }
                />
              </List.Item>
            )}
          />
        )
      )}
      {/* All authenticated users can add comments */}
      <Space.Compact style={{ width: '100%', marginTop: 6 }}>
          <Input
            size="small"
            placeholder={t('commentPlaceholder')}
            value={newComment}
            onChange={e => setNewComment(e.target.value)}
            onPressEnter={handleSend}
          />
          <Button size="small" type="primary" loading={sending} onClick={handleSend}>{t('commentSend')}</Button>
        </Space.Compact>
    </div>
  );
}

export default function FileDetailPage() {
  const { id } = useParams();
  const navigate = useNavigate();
  const { t } = useLang();
  const { canEdit, user, isAdmin } = useAuth();

  const [file, setFile] = useState(null);
  const [loading, setLoading] = useState(true);
  const [selectedVersions, setSelectedVersions] = useState([]);
  const [diff, setDiff] = useState(null);
  const [diffLoading, setDiffLoading] = useState(false);

  const [uploadModal, setUploadModal] = useState(false);
  const [uploadLoading, setUploadLoading] = useState(false);
  const [fileList, setFileList] = useState([]);
  const [form] = Form.useForm();

  const [lockLoading, setLockLoading] = useState(false);
  const [lockModal, setLockModal] = useState(false);
  const [lockReason, setLockReason] = useState('');

  const [isDiffExpanded, setIsDiffExpanded] = useState(false);

  // Tags
  const [allTags, setAllTags] = useState([]);
  const [tagModal, setTagModal] = useState(false);
  const [createTagModal, setCreateTagModal] = useState(false);
  const [tagForm] = Form.useForm();
  const [tagLoading, setTagLoading] = useState(false);
  const [selectedTagIds, setSelectedTagIds] = useState([]);

  // Subscribe
  const [subscribeLoading, setSubscribeLoading] = useState(false);

  // Image preview
  const [previewBlobUrls, setPreviewBlobUrls] = useState({});

  const fetchFile = async () => {
    setLoading(true);
    try {
      const res = await api.get(`/files/${id}`);
      setFile(res.data);
      setSelectedTagIds((res.data.tags || []).map(t => t.id));
    } catch {
      message.error(t('error'));
    } finally {
      setLoading(false);
    }
  };

  const fetchAllTags = useCallback(async () => {
    try {
      const res = await api.get('/tags');
      setAllTags(res.data);
    } catch { /* ignore */ }
  }, []);

  useEffect(() => { fetchFile(); fetchAllTags(); }, [id]);

  // Load preview blob URLs for image/video/audio versions
  useEffect(() => {
    if (!file) return;
    const MAX_MEDIA_PREVIEW_SIZE_BYTES = 200 * 1024 * 1024;
    const previewableVersions = file.versions.filter(v => isPreviewableMime(v.mimeType));
    const pendingUrls = [];
    let cancelled = false;

    (async () => {
      for (const v of previewableVersions) {
        if (cancelled) break;
        if (previewBlobUrls[v.id]) continue;
        // Skip video/audio files larger than MAX_MEDIA_PREVIEW_SIZE_BYTES to avoid memory issues
        if ((isVideoMime(v.mimeType) || isAudioMime(v.mimeType)) && v.size && v.size > MAX_MEDIA_PREVIEW_SIZE_BYTES) continue;
        try {
          const res = await api.get(`/versions/${v.id}/preview`, { responseType: 'blob', timeout: 60000 });
          if (cancelled) break;
          const url = URL.createObjectURL(res.data);
          pendingUrls.push(url);
          setPreviewBlobUrls(prev => ({ ...prev, [v.id]: url }));
        } catch { /* ignore */ }
      }
    })();

    return () => {
      cancelled = true;
      pendingUrls.forEach(url => URL.revokeObjectURL(url));
    };
  }, [file?.versions?.length]);


  const handleSelectVersion = (version) => {
    setSelectedVersions(prev => {
      if (prev.find(v => v.id === version.id)) {
        return prev.filter(v => v.id !== version.id);
      }
      if (prev.length >= 2) {
        return [prev[1], version];
      }
      return [...prev, version];
    });
  };

  const handleDiff = async () => {
    if (selectedVersions.length < 2) {
      message.warning(t('selectVersionsToCompare'));
      return;
    }
    const [from, to] = selectedVersions.sort((a, b) => a.versionNumber - b.versionNumber);
    setDiffLoading(true);
    setDiff(null);
    try {
      const res = await api.get('/versions/diff', {
        params: { fromId: from.id, toId: to.id },
      });
      setDiff(res.data);
    } catch {
      message.error(t('error'));
    } finally {
      setDiffLoading(false);
    }
  };

  const handleDownload = async (versionId) => {
    try {
      const res = await api.get(`/versions/${versionId}/download`, {
        responseType: 'blob',
        timeout: 0,
      });
      const disposition = res.headers['content-disposition'];
      let fileName = 'download';
      if (disposition) {
        const match = disposition.match(/filename\*?=(?:UTF-8'')?["']?([^"';\n]+)/i);
        if (match) fileName = decodeURIComponent(match[1]);
      }
      const url = window.URL.createObjectURL(new Blob([res.data]));
      const link = document.createElement('a');
      link.href = url;
      link.setAttribute('download', fileName);
      document.body.appendChild(link);
      link.click();
      link.remove();
      window.URL.revokeObjectURL(url);
      // Track that this file was downloaded and may need re-uploading
      const version = file?.versions?.find(v => v.id === versionId);
      addPendingUpload({
        fileId: file.id,
        fileName: file.name,
        versionId,
        versionNumber: version?.versionNumber,
      });
    } catch (err) {
      message.error(err.response?.data?.message || t('error'));
    }
  };

  const handleRestore = async (versionId, versionNumber) => {
    try {
      await api.post(`/versions/${versionId}/restore`);
      message.success(t('restoreSuccess'));
      fetchFile();
    } catch (err) {
      message.error(err.response?.data?.message || t('error'));
    }
  };

  const handleUpload = async (values) => {
    if (!fileList.length) {
      message.error(t('selectFile'));
      return;
    }
    setUploadLoading(true);
    try {
      const formData = new FormData();
      formData.append('file', fileList[0].originFileObj);
      if (values.commitMessage) formData.append('commitMessage', values.commitMessage);
      if (file.folderId) {
        formData.append('folderId', file.folderId);
      } else {
        formData.append('filePath', file.path);
      }

      await api.post('/files', formData, {
        headers: { 'Content-Type': 'multipart/form-data' },
      });
      removePendingUpload(file.id);
      message.success(t('fileUploaded'));
      setUploadModal(false);
      form.resetFields();
      setFileList([]);
      fetchFile();
    } catch (err) {
      message.error(err.response?.data?.message || t('error'));
    } finally {
      setUploadLoading(false);
    }
  };

  if (loading) {
    return <div style={{ display: 'flex', justifyContent: 'center', paddingTop: 100 }}><Spin size="large" /></div>;
  }

  if (!file) return null;

  const isLockedByMe = file.lockedBy === user?.id;
  const isLockedByOther = file.lockedBy && !isLockedByMe;
  const canUnlock = isLockedByMe || isAdmin;

  const handleLock = async () => {
    setLockLoading(true);
    try {
      await lockFile(file.id, { reason: lockReason });
      message.success(t('lockSuccess'));
      setLockModal(false);
      setLockReason('');
      fetchFile();
    } catch (err) {
      message.error(err.response?.data?.message || t('error'));
    } finally {
      setLockLoading(false);
    }
  };

  const handleUnlock = async () => {
    setLockLoading(true);
    try {
      await unlockFile(file.id);
      message.success(t('unlockSuccess'));
      fetchFile();
    } catch (err) {
      message.error(err.response?.data?.message || t('error'));
    } finally {
      setLockLoading(false);
    }
  };

  // Tag handlers
  const handleSaveTags = async () => {
    setTagLoading(true);
    try {
      await api.post(`/files/${file.id}/tags`, { tagIds: selectedTagIds });
      // Remove tags not in selection
      for (const existingTag of file.tags || []) {
        if (!selectedTagIds.includes(existingTag.id)) {
          await api.delete(`/files/${file.id}/tags/${existingTag.id}`);
        }
      }
      message.success(t('tagAdded'));
      setTagModal(false);
      fetchFile();
    } catch (err) {
      message.error(err.response?.data?.message || t('error'));
    } finally {
      setTagLoading(false);
    }
  };

  const handleCreateTag = async (values) => {
    setTagLoading(true);
    try {
      const color = typeof values.color === 'string' ? values.color : (values.color?.toHexString ? values.color.toHexString() : '#1677ff');
      await api.post('/tags', { name: values.name, color });
      message.success(t('tagCreated'));
      tagForm.resetFields();
      setCreateTagModal(false);
      fetchAllTags();
    } catch (err) {
      message.error(err.response?.data?.message || t('error'));
    } finally {
      setTagLoading(false);
    }
  };

  const handleDeleteTag = async (tagId) => {
    try {
      await api.delete(`/tags/${tagId}`);
      message.success(t('tagDeleted'));
      fetchAllTags();
      fetchFile();
    } catch (err) {
      message.error(err.response?.data?.message || t('error'));
    }
  };

  // Subscribe handlers
  const handleSubscribe = async () => {
    setSubscribeLoading(true);
    try {
      if (file.isSubscribed) {
        await api.delete(`/files/${file.id}/subscribe`);
        message.success(t('unsubscribeSuccess'));
      } else {
        await api.post(`/files/${file.id}/subscribe`);
        message.success(t('subscribeSuccess'));
      }
      fetchFile();
    } catch (err) {
      message.error(err.response?.data?.message || t('error'));
    } finally {
      setSubscribeLoading(false);
    }
  };

  const latestVersion = file.versions[0];
  const hasPreviewableVersions = file.versions.some(v => isPreviewableMime(v.mimeType));


  const tabItems = [
    {
      key: 'graph',
      label: t('versionHistory'),
      children: (
        <div style={{ maxHeight: 500, overflow: 'auto' }}>
          <CommitGraph
            versions={file.versions}
            onSelectVersion={handleSelectVersion}
            selectedVersionIds={selectedVersions.map(v => v.id)}
          />
        </div>
      ),
    },
    {
      key: 'diff',
      label: (
        <span>
          {t('diffView')}
          {selectedVersions.length === 2 && <Badge count={2} size="small" style={{ marginLeft: 6 }} />}
        </span>
      ),
      children: (
        <div>
          {selectedVersions.length < 2 ? (
            <div style={{ textAlign: 'center', padding: '32px 0', color: '#8c8c8c' }}>
              <SwapOutlined style={{ fontSize: 32, marginBottom: 8, display: 'block' }} />
              {t('selectVersionsToCompare')}
            </div>
          ) : (
            <div>
              <div style={{ marginBottom: 16, display: 'flex', alignItems: 'center', gap: 12 }}>
                <Text>
                  Comparing:
                  {' '}
                  <Tag color="blue">v{Math.min(...selectedVersions.map(v => v.versionNumber))}</Tag>
                  →
                  <Tag color="green">v{Math.max(...selectedVersions.map(v => v.versionNumber))}</Tag>
                </Text>
                <Button
                  type="primary"
                  size="small"
                  icon={<SwapOutlined />}
                  loading={diffLoading}
                  onClick={handleDiff}
                >
                  {t('compare')}
                </Button>
                {diff && (
                  <Tooltip title={isDiffExpanded ? t('collapse') ?? 'Thu gọn' : t('expand') ?? 'Mở rộng'}>
                    <Button
                      size="small"
                      icon={isDiffExpanded ? <ShrinkOutlined /> : <ExpandOutlined />}
                      onClick={() => setIsDiffExpanded(v => !v)}
                    />
                  </Tooltip>
                )}
              </div>
              {diff && (
                <FileDiff
                  diff={diff.diff}
                  isBinary={diff.isBinary}
                  isOfficeExtracted={diff.isOfficeExtracted}
                  fromVersion={diff.from}
                  toVersion={diff.to}
                />
              )}
            </div>
          )}
        </div>
      ),
    },
    ...(hasPreviewableVersions ? [{
      key: 'preview',
      label: t('preview'),
      children: (
        <div>
          <Row gutter={16}>
            {file.versions.filter(v => isPreviewableMime(v.mimeType)).slice(0, 6).map(v => (
              <Col key={v.id} xs={24} sm={12} md={isAudioMime(v.mimeType) ? 24 : 8} style={{ marginBottom: 16 }}>
                <Card size="small" title={<Space><Tag color="blue">v{v.versionNumber}</Tag><Text style={{ fontSize: 12 }}>{v.uploadedBy}</Text></Space>}>
                  {isImageMime(v.mimeType) && (
                    previewBlobUrls[v.id] ? (
                      <Image
                        src={previewBlobUrls[v.id]}
                        style={{ maxWidth: '100%', maxHeight: 200, objectFit: 'contain' }}
                        preview={{ src: previewBlobUrls[v.id] }}
                      />
                    ) : (
                      <div style={{ textAlign: 'center', padding: 20 }}><Spin /></div>
                    )
                  )}
                  {isVideoMime(v.mimeType) && (
                    previewBlobUrls[v.id] ? (
                      <video
                        src={previewBlobUrls[v.id]}
                        controls
                        style={{ maxWidth: '100%', maxHeight: 200 }}
                      />
                    ) : (
                      <div style={{ textAlign: 'center', padding: 20 }}><Spin /></div>
                    )
                  )}
                  {isAudioMime(v.mimeType) && (
                    previewBlobUrls[v.id] ? (
                      <audio
                        src={previewBlobUrls[v.id]}
                        controls
                        style={{ width: '100%' }}
                      />
                    ) : (
                      <div style={{ textAlign: 'center', padding: 20 }}><Spin /></div>
                    )
                  )}
                  <div style={{ marginTop: 8, fontSize: 12, color: '#8c8c8c' }}>{dayjs(v.createdAt).format('YYYY-MM-DD HH:mm')}</div>
                </Card>
              </Col>
            ))}
          </Row>
        </div>
      ),
    }] : []),
  ];

  return (
    <div>
      <div style={{ marginBottom: 16 }}>
        <Button icon={<ArrowLeftOutlined />} onClick={() => navigate('/files')}>
          {t('back')}
        </Button>
      </div>

      {/* Lock status alert */}
      {isLockedByOther && (
        <Alert
          type="warning"
          showIcon
          icon={<LockOutlined />}
          style={{ marginBottom: 16 }}
          message={t('fileLocked')}
          description={`${t('fileLockedBy')}: ${file.lockedByName || file.lockedBy}${file.lockedAt ? ` (${dayjs(file.lockedAt).format('YYYY-MM-DD HH:mm')})` : ''}${file.lockReason ? ` — ${file.lockReason}` : ''}`}
        />
      )}

      {/* File info */}
      <Card style={{ marginBottom: 16 }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
          <div>
            <Space align="center" style={{ marginBottom: 4 }}>
              <Title level={3} style={{ margin: 0 }}>{file.name}</Title>
              {file.lockedBy && (
                <Tooltip title={isLockedByMe ? t('fileLocked') : `${t('fileLockedBy')}: ${file.lockedByName}`}>
                  <Tag color={isLockedByMe ? 'orange' : 'red'} icon={<LockOutlined />}>
                    {isLockedByMe ? t('fileLocked') : file.lockedByName}
                  </Tag>
                </Tooltip>
              )}
            </Space>
            {file.folderPath ? (
              <Text type="secondary">
                <FolderOutlined style={{ marginRight: 4 }} />
                {file.folderPath} / {file.name}
              </Text>
            ) : (
              <Text type="secondary">{file.path}</Text>
            )}
            {file.description && (
              <div style={{ marginTop: 8 }}>
                <Text>{file.description}</Text>
              </div>
            )}
          </div>
          <Space>
            {/* Subscribe button */}
            <Button
              icon={file.isSubscribed ? <BellFilled style={{ color: '#1677ff' }} /> : <BellOutlined />}
              loading={subscribeLoading}
              onClick={handleSubscribe}
            >
              {file.isSubscribed ? t('subscribed') : t('subscribe')}
            </Button>
            {canEdit && !file.lockedBy && (
              <Button
                icon={<LockOutlined />}
                onClick={() => setLockModal(true)}
              >
                {t('lockFile')}
              </Button>
            )}
            {canUnlock && file.lockedBy && (
              <Popconfirm
                title={t('confirmUnlock')}
                onConfirm={handleUnlock}
                okText={t('yes')}
                cancelText={t('no')}
              >
                <Button icon={<UnlockOutlined />} loading={lockLoading}>
                  {t('unlockFile')}
                </Button>
              </Popconfirm>
            )}
            {canEdit && (
              <Button
                type="primary"
                icon={<UploadOutlined />}
                onClick={() => setUploadModal(true)}
                disabled={isLockedByOther}
                title={isLockedByOther ? t('cannotUploadLocked') : undefined}
              >
                {t('uploadNewVersion')}
              </Button>
            )}
          </Space>
        </div>

        {/* Tags display */}
        <div style={{ marginTop: 12 }}>
          <Space wrap>
            {(file.tags || []).map(tag => (
              <Tag key={tag.id} color={tag.color} style={{ marginBottom: 4 }}>{tag.name}</Tag>
            ))}
            {canEdit && (
              <Button
                size="small"
                icon={<TagOutlined />}
                onClick={() => { setSelectedTagIds((file.tags || []).map(t => t.id)); setTagModal(true); }}
              >
                {t('manageTags')}
              </Button>
            )}
          </Space>
        </div>

        <Divider style={{ margin: '16px 0' }} />

        <Row gutter={16}>
          <Col span={6}>
            <Text type="secondary">{t('versions')}</Text>
            <div><Tag color="blue" icon={<TagOutlined />}>{file.versions.length} versions</Tag></div>
          </Col>
          <Col span={6}>
            <Text type="secondary">{t('fileSize')}</Text>
            <div><Text strong>{formatBytes(latestVersion?.size)}</Text></div>
          </Col>
          <Col span={6}>
            <Text type="secondary">{t('createdBy')}</Text>
            <div><Text>{file.createdBy}</Text></div>
          </Col>
          <Col span={6}>
            <Text type="secondary">{t('updatedAt')}</Text>
            <div><Text>{dayjs(file.updatedAt).format('YYYY-MM-DD HH:mm')}</Text></div>
          </Col>
        </Row>

        {latestVersion && (
          <>
            <Divider style={{ margin: '16px 0' }} />
            <Row gutter={16}>
              <Col span={24}>
                <Text type="secondary">{t('checksum')}</Text>
                <div>
                  <Text code style={{ fontSize: 12 }}>{latestVersion.checksum}</Text>
                </div>
              </Col>
            </Row>
          </>
        )}
      </Card>

      {/* Version tabs */}
      <Row gutter={16}>
        <Col xs={24} lg={14}>
          <Card>
            <Tabs items={tabItems} />
          </Card>
        </Col>

        <Col xs={24} lg={10}>
          <Card title={t('versionHistory')} style={{ maxHeight: 600, overflow: 'auto' }}>
            {(() => {
              const groups = {};
              file.versions.forEach(v => {
                const date = dayjs(v.createdAt).format('YYYY-MM-DD');
                if (!groups[date]) groups[date] = [];
                groups[date].push(v);
              });
              const sortedDates = Object.keys(groups).sort((a, b) => b.localeCompare(a));
              return (
                <div style={{ position: 'relative' }}>
                  {/* Vertical trunk line */}
                  <div style={{ position: 'absolute', left: 5, top: 0, bottom: 0, width: 2, backgroundColor: '#d9d9d9', zIndex: 0 }} />
                  {sortedDates.map(date => {
                    const dateVersions = groups[date].slice().sort((a, b) => b.versionNumber - a.versionNumber);
                    return (
                      <div key={date} style={{ marginBottom: 8, position: 'relative' }}>
                        {/* Date node row */}
                        <div style={{ display: 'flex', alignItems: 'center', marginBottom: 6 }}>
                          <div style={{ width: 12, height: 12, borderRadius: '50%', backgroundColor: '#1677ff', border: '2px solid #fff', flexShrink: 0, position: 'relative', zIndex: 1 }} />
                          <Text strong style={{ fontSize: 13, marginLeft: 8 }}>{date}</Text>
                        </div>
                        {/* Version nodes */}
                        {dateVersions.map(v => {
                          const isLatest = v.versionNumber === file.versions[0]?.versionNumber;
                          const isSelected = !!selectedVersions.find(sv => sv.id === v.id);
                          return (
                            <div key={v.id} style={{ display: 'flex', alignItems: 'flex-start', marginBottom: 4 }}>
                              {/* Gutter with horizontal connector line */}
                              <div style={{ width: 24, flexShrink: 0, position: 'relative', paddingTop: 1 }}>
                                <div style={{ position: 'absolute', top: 10, left: 6, width: 18, height: 2, backgroundColor: '#d9d9d9' }} />
                              </div>
                              {/* Version circle */}
                              <div style={{ width: 8, height: 8, borderRadius: '50%', backgroundColor: '#52c41a', flexShrink: 0, marginTop: 6, marginRight: 8, position: 'relative', zIndex: 1 }} />
                              {/* Version info */}
                              <div
                                onClick={() => handleSelectVersion(v)}
                                style={{ flex: 1, padding: '4px 6px', borderRadius: 6, cursor: 'pointer', backgroundColor: isSelected ? '#e6f4ff' : 'transparent', border: `1px solid ${isSelected ? '#91caff' : 'transparent'}`, minWidth: 0 }}
                              >
                                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', gap: 4 }}>
                                  <Space size={4} wrap>
                                    <Tag color="blue" style={{ margin: 0 }}>v{v.versionNumber}</Tag>
                                    {isImageMime(v.mimeType) && previewBlobUrls[v.id] && (
                                      <img src={previewBlobUrls[v.id]} alt="thumb" style={{ width: 24, height: 24, objectFit: 'cover', borderRadius: 2, verticalAlign: 'middle' }} />
                                    )}
                                    {isVideoMime(v.mimeType) && (
                                      <span style={{ fontSize: 16 }}>🎬</span>
                                    )}
                                    {isAudioMime(v.mimeType) && (
                                      <span style={{ fontSize: 16 }}>🎵</span>
                                    )}
                                    <Text style={{ fontSize: 12 }}>{v.commitMessage || `Version ${v.versionNumber}`}</Text>
                                  </Space>
                                  <Space size={2} style={{ flexShrink: 0 }} onClick={e => e.stopPropagation()}>
                                    <Tooltip title={t('download')}>
                                      <Button size="small" type="text" icon={<DownloadOutlined />} onClick={() => handleDownload(v.id)} />
                                    </Tooltip>
                                    {canEdit && v.versionNumber !== file.versions[0]?.versionNumber && (
                                      <Popconfirm
                                        title={t('restoreConfirm', { version: v.versionNumber })}
                                        onConfirm={() => handleRestore(v.id, v.versionNumber)}
                                        okText={t('yes')}
                                        cancelText={t('no')}
                                      >
                                        <Tooltip title={t('restoreVersion')}>
                                          <Button size="small" type="text" icon={<RollbackOutlined />} />
                                        </Tooltip>
                                      </Popconfirm>
                                    )}
                                  </Space>
                                </div>
                                <div style={{ display: 'flex', gap: 8, marginTop: 2 }}>
                                  <Text type="secondary" style={{ fontSize: 11 }}>{v.uploadedBy}</Text>
                                  <Text type="secondary" style={{ fontSize: 11 }}>{dayjs(v.createdAt).format('HH:mm')}</Text>
                                  <Text type="secondary" style={{ fontSize: 11 }}>{formatBytes(v.size)}</Text>
                                </div>
                                {/* Version comments */}
                                <div onClick={e => e.stopPropagation()}>
                                  <VersionComments
                                    versionId={v.id}
                                    user={user}
                                    isAdmin={isAdmin}
                                    canEdit={canEdit}
                                    t={t}
                                  />
                                </div>
                              </div>
                            </div>
                          );
                        })}
                      </div>
                    );
                  })}
                </div>
              );
            })()}
          </Card>
        </Col>
      </Row>

      {/* Upload new version modal */}
      <Modal
        title={t('uploadNewVersion')}
        open={uploadModal}
        onCancel={() => { setUploadModal(false); form.resetFields(); setFileList([]); }}
        footer={null}
        width={520}
      >
        <Form form={form} layout="vertical" onFinish={handleUpload}>
          <Form.Item label={t('selectFile')} required>
            <Dragger
              fileList={fileList}
              beforeUpload={() => false}
              onChange={({ fileList: fl }) => setFileList(fl.slice(-1))}
              maxCount={1}
            >
              <p className="ant-upload-drag-icon"><InboxOutlined /></p>
              <p className="ant-upload-text">{t('uploadHint')}</p>
            </Dragger>
          </Form.Item>

          <Form.Item name="commitMessage" label={`${t('commitMessage')} ${t('optional')}`}>
            <Input.TextArea rows={3} placeholder={t('commitMessagePlaceholder')} />
          </Form.Item>

          <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 8 }}>
            <Button onClick={() => { setUploadModal(false); form.resetFields(); setFileList([]); }}>
              {t('cancel')}
            </Button>
            <Button type="primary" htmlType="submit" loading={uploadLoading} icon={<UploadOutlined />}>
              {t('upload')}
            </Button>
          </div>
        </Form>
      </Modal>

      {/* Lock file modal */}
      <Modal
        title={t('lockFile')}
        open={lockModal}
        onCancel={() => { setLockModal(false); setLockReason(''); }}
        onOk={handleLock}
        confirmLoading={lockLoading}
        okText={t('confirm')}
        cancelText={t('cancel')}
      >
        <Input.TextArea
          rows={3}
          placeholder={t('lockReasonPlaceholder')}
          value={lockReason}
          onChange={(e) => setLockReason(e.target.value)}
        />
      </Modal>

      {/* Diff fullscreen expand modal */}
      <Modal
        open={isDiffExpanded}
        onCancel={() => setIsDiffExpanded(false)}
        footer={null}
        width="95vw"
        style={{ top: 20 }}
        styles={{ body: { maxHeight: '85vh', overflow: 'auto', padding: '0 16px' } }}
        title={
          diff && selectedVersions.length >= 2 && (
            <span>
              Comparing:
              {' '}
              <Tag color="blue">v{Math.min(...selectedVersions.map(v => v.versionNumber))}</Tag>
              →
              <Tag color="green">v{Math.max(...selectedVersions.map(v => v.versionNumber))}</Tag>
            </span>
          )
        }
        destroyOnClose={false}
      >
        {diff && (
          <FileDiff
            diff={diff.diff}
            isBinary={diff.isBinary}
            isOfficeExtracted={diff.isOfficeExtracted}
            fromVersion={diff.from}
            toVersion={diff.to}
            isExpanded
          />
        )}
      </Modal>

      {/* Manage tags modal */}
      <Modal
        title={t('manageTags')}
        open={tagModal}
        onCancel={() => setTagModal(false)}
        onOk={handleSaveTags}
        okText={t('save')}
        cancelText={t('cancel')}
        confirmLoading={tagLoading}
      >
        <div style={{ marginBottom: 12 }}>
          <Select
            mode="multiple"
            style={{ width: '100%' }}
            placeholder={t('selectTags')}
            value={selectedTagIds}
            onChange={setSelectedTagIds}
            options={allTags.map(tag => ({
              value: tag.id,
              label: <Space><span style={{ display: 'inline-block', width: 12, height: 12, borderRadius: '50%', background: tag.color }} />{tag.name}</Space>,
            }))}
          />
        </div>
        {canEdit && (
          <Button size="small" icon={<PlusOutlined />} onClick={() => setCreateTagModal(true)}>
            {t('createTag')}
          </Button>
        )}
        {isAdmin && allTags.length > 0 && (
          <div style={{ marginTop: 8 }}>
            <Text type="secondary" style={{ fontSize: 12 }}>{t('tags')}:</Text>
            <div style={{ marginTop: 4 }}>
              {allTags.map(tag => (
                <Space key={tag.id} style={{ marginRight: 8, marginBottom: 4 }}>
                  <Tag color={tag.color}>{tag.name}</Tag>
                  <Popconfirm
                    title={t('deleteTagConfirm')}
                    onConfirm={() => handleDeleteTag(tag.id)}
                    okText={t('yes')}
                    cancelText={t('no')}
                  >
                    <Button type="text" size="small" danger icon={<DeleteOutlined />} />
                  </Popconfirm>
                </Space>
              ))}
            </div>
          </div>
        )}
      </Modal>

      {/* Create tag modal */}
      <Modal
        title={t('createTag')}
        open={createTagModal}
        onCancel={() => { setCreateTagModal(false); tagForm.resetFields(); }}
        onOk={() => tagForm.submit()}
        okText={t('create')}
        cancelText={t('cancel')}
        confirmLoading={tagLoading}
      >
        <Form form={tagForm} layout="vertical" onFinish={handleCreateTag}>
          <Form.Item name="name" label={t('tagName')} rules={[{ required: true }]}>
            <Input />
          </Form.Item>
          <Form.Item name="color" label={t('tagColor')} initialValue="#1677ff">
            <div>
              <Space wrap>
                {PRESET_COLORS.map(c => (
                  <div
                    key={c}
                    onClick={() => tagForm.setFieldValue('color', c)}
                    style={{
                      width: 24, height: 24, borderRadius: '50%', background: c, cursor: 'pointer',
                      border: tagForm.getFieldValue('color') === c ? '3px solid #000' : '2px solid transparent',
                    }}
                  />
                ))}
              </Space>
            </div>
          </Form.Item>
        </Form>
      </Modal>
    </div>
  );
}
