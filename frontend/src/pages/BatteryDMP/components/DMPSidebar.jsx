import React, { useEffect, useMemo, useState } from 'react';
import { Alert, Input, Spin, Tree } from 'antd';
import { fetchBatches, fetchChannels, fetchChanges } from '../../../api/dmpApi';
import { useLang } from '../../../contexts/LangContext';

function buildBatchTree(batches, channelsByBatch, labels) {
  const dateMap = new Map();

  batches.forEach((batch) => {
    const date = batch.fdrq || labels.unknownDate;
    const model = batch.dcxh || labels.unknownModel;
    const pattern = batch.fdfs || labels.unknownPattern;

    if (!dateMap.has(date)) dateMap.set(date, new Map());
    const modelMap = dateMap.get(date);
    if (!modelMap.has(model)) modelMap.set(model, new Map());
    const patternMap = modelMap.get(model);
    if (!patternMap.has(pattern)) patternMap.set(pattern, []);
    patternMap.get(pattern).push(batch);
  });

  return Array.from(dateMap.entries()).map(([date, modelMap]) => ({
    key: `date:${date}`,
    title: date,
    selectable: false,
    children: Array.from(modelMap.entries()).map(([model, patternMap]) => ({
      key: `model:${date}:${model}`,
      title: model,
      selectable: false,
      children: Array.from(patternMap.entries()).map(([pattern, batchList]) => ({
        key: `pattern:${date}:${model}:${pattern}`,
        title: pattern,
        selectable: false,
        children: batchList.map((batch) => {
          const batchKey = `batch:${batch.id}`;
          const channels = channelsByBatch[batch.id] || [];
          return {
            key: batchKey,
            title: `${labels.batch} ${batch.id}`,
            selectable: false,
            batchId: batch.id,
            model,
            date,
            children: channels.length
              ? channels.map((channel) => ({
                key: `channel:${batch.id}:${channel.baty}`,
                title: `${labels.channel} ${channel.baty}`,
                isLeaf: true,
                selectable: true,
                selection: {
                  batchId: batch.id,
                  cdmc: channel.cdmc,
                  channel: channel.baty,
                  model,
                  date,
                },
              }))
              : [{ key: `placeholder:${batch.id}`, title: labels.clickToLoad, selectable: false, disabled: true }],
          };
        }),
      })),
    })),
  }));
}

function filterTree(nodes, keyword) {
  if (!keyword) return nodes;
  const lowerKeyword = keyword.toLowerCase();

  return nodes
    .map((node) => {
      const title = String(node.title || '').toLowerCase();
      const children = node.children ? filterTree(node.children, keyword) : [];
      if (title.includes(lowerKeyword) || children.length > 0) {
        return { ...node, children };
      }
      return null;
    })
    .filter(Boolean);
}

export default function DMPSidebar({ stationId, onSelect }) {
  const { t } = useLang();
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [channelError, setChannelError] = useState('');
  const [searchValue, setSearchValue] = useState('');
  const [batches, setBatches] = useState([]);
  const [channelsByBatch, setChannelsByBatch] = useState({});
  const [expandedKeys, setExpandedKeys] = useState([]);

  useEffect(() => {
    onSelect?.(null);
    setBatches([]);
    setChannelsByBatch({});
    setExpandedKeys([]);
    setError('');
    setChannelError('');

    if (!stationId) {
      setLoading(false);
      return () => {};
    }

    let mounted = true;
    setLoading(true);
    setError('');

    fetchBatches(stationId)
      .then((result) => {
        if (!mounted) return;
        setBatches(result);
      })
      .catch((err) => {
        if (!mounted) return;
        setError(err.message || 'Failed to load batches');
      })
      .finally(() => {
        if (!mounted) return;
        setLoading(false);
      });

    return () => {
      mounted = false;
    };
  }, [stationId, onSelect]);

  useEffect(() => {
    if (!stationId) return () => {};

    let active = true;
    let since = 0;

    const pollChanges = async () => {
      try {
        const { changes, timestamp } = await fetchChanges(stationId, since);
        since = timestamp;
        if (!active) return;
        setError('');
        if (!changes.length) return;
        const updatedBatches = await fetchBatches(stationId);
        if (!active) return;
        setBatches(updatedBatches);
        setChannelsByBatch((prev) => {
          const validBatchIds = new Set(updatedBatches.map((batch) => String(batch.id)));
          return Object.fromEntries(
            Object.entries(prev).filter(([batchId]) => validBatchIds.has(String(batchId))),
          );
        });
      } catch (err) {
        if (!active) return;
        setError(err.message || 'Connection to DMP station failed');
      }
    };

    pollChanges();
    const intervalId = setInterval(pollChanges, 30000);
    return () => {
      active = false;
      clearInterval(intervalId);
    };
  }, [stationId]);

  const treeData = useMemo(() => buildBatchTree(batches, channelsByBatch, {
    unknownModel: t('dmpUnknownModel'),
    unknownDate: t('dmpUnknownDate'),
    unknownPattern: t('dmpUnknownPattern'),
    batch: t('dmpBatch'),
    channel: t('dmpChannel'),
    clickToLoad: t('dmpClickToLoadChannels'),
  }), [batches, channelsByBatch, t]);
  const filteredTreeData = useMemo(() => filterTree(treeData, searchValue), [treeData, searchValue]);

  const handleExpand = async (nextExpandedKeys, info) => {
    setExpandedKeys(nextExpandedKeys);
    const node = info.node;
    if (!stationId || !node?.key?.startsWith('batch:')) return;

    const batchId = node.batchId;
    if (channelsByBatch[batchId]) return;

    setChannelError('');
    try {
      const channels = await fetchChannels(stationId, batchId);
      setChannelsByBatch((prev) => ({ ...prev, [batchId]: channels }));
    } catch (err) {
      setChannelError(err.message || 'Failed to load channels');
    }
  };

  const handleSelect = (_keys, info) => {
    if (info?.node?.selection && onSelect) {
      onSelect(info.node.selection);
    }
  };

  if (!stationId) {
    return <Alert type="info" showIcon message={t('dmpSelectStationToBrowse')} />;
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%', gap: 12 }}>
      <Input.Search
        allowClear
        placeholder={t('dmpSearchPlaceholder')}
        value={searchValue}
        onChange={(event) => setSearchValue(event.target.value)}
      />

      {error && <Alert type="error" message={error} showIcon />}
      {channelError && <Alert type="error" message={channelError} showIcon />}

      <div style={{ flex: 1, overflow: 'auto' }}>
        {loading ? (
          <div style={{ display: 'flex', justifyContent: 'center', paddingTop: 32 }}>
            <Spin />
          </div>
        ) : (
          <Tree
            blockNode
            treeData={filteredTreeData}
            expandedKeys={expandedKeys}
            onExpand={handleExpand}
            onSelect={handleSelect}
          />
        )}
      </div>
    </div>
  );
}
