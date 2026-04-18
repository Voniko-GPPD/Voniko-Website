import React, { useEffect, useMemo, useState } from 'react';
import { Alert, Input, Spin, Tree } from 'antd';
import { fetchBatches, fetchChannels } from '../../../api/dmpApi';

function buildBatchTree(batches, channelsByBatch) {
  const modelMap = new Map();

  batches.forEach((batch) => {
    const model = batch.dcxh || 'Unknown Model';
    const date = batch.fdrq || 'Unknown Date';

    if (!modelMap.has(model)) modelMap.set(model, new Map());
    const dateMap = modelMap.get(model);
    if (!dateMap.has(date)) dateMap.set(date, []);
    dateMap.get(date).push(batch);
  });

  return Array.from(modelMap.entries()).map(([model, dateMap]) => ({
    key: `model:${model}`,
    title: model,
    selectable: false,
    children: Array.from(dateMap.entries()).map(([date, modelBatches]) => ({
      key: `date:${model}:${date}`,
      title: date,
      selectable: false,
      children: modelBatches.map((batch) => {
        const batchKey = `batch:${batch.id}`;
        const channels = channelsByBatch[batch.id] || [];
        return {
          key: batchKey,
          title: `Batch ${batch.id}`,
          selectable: false,
          batchId: batch.id,
          model,
          date,
          children: channels.length
            ? channels.map((channel) => ({
              key: `channel:${batch.id}:${channel.baty}`,
              title: `Channel ${channel.baty}`,
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
            : [{ key: `placeholder:${batch.id}`, title: 'Click to load channels', selectable: false, disabled: true }],
        };
      }),
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
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [searchValue, setSearchValue] = useState('');
  const [batches, setBatches] = useState([]);
  const [channelsByBatch, setChannelsByBatch] = useState({});
  const [expandedKeys, setExpandedKeys] = useState([]);

  useEffect(() => {
    onSelect?.(null);
    setBatches([]);
    setChannelsByBatch({});
    setExpandedKeys([]);

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

  const treeData = useMemo(() => buildBatchTree(batches, channelsByBatch), [batches, channelsByBatch]);
  const filteredTreeData = useMemo(() => filterTree(treeData, searchValue), [treeData, searchValue]);

  const handleExpand = async (nextExpandedKeys, info) => {
    setExpandedKeys(nextExpandedKeys);
    const node = info.node;
    if (!stationId || !node?.key?.startsWith('batch:')) return;

    const batchId = node.batchId;
    if (channelsByBatch[batchId]) return;

    try {
      const channels = await fetchChannels(stationId, batchId);
      setChannelsByBatch((prev) => ({ ...prev, [batchId]: channels }));
    } catch (err) {
      setError(err.message || 'Failed to load channels');
    }
  };

  const handleSelect = (_keys, info) => {
    if (info?.node?.selection && onSelect) {
      onSelect(info.node.selection);
    }
  };

  if (!stationId) {
    return <Alert type="info" showIcon message="Select a DMP station to browse batches." />;
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%', gap: 12 }}>
      <Input.Search
        allowClear
        placeholder="Search model/date/batch/channel"
        value={searchValue}
        onChange={(event) => setSearchValue(event.target.value)}
      />

      {error && <Alert type="error" message={error} showIcon />}

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
