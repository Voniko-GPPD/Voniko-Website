import React, { useEffect, useMemo, useState } from 'react';
import {
  Alert,
  Card,
  Select,
  Tabs,
  Tag,
  Typography,
} from 'antd';
import { ExperimentOutlined, DatabaseOutlined } from '@ant-design/icons';
import { fetchStations } from '../../api/dmpApi';
import { useLang } from '../../contexts/LangContext';
import { useAuth } from '../../contexts/AuthContext';
import DMPFilterPanel from './components/DMPFilterPanel';
import DMPChartTab from './components/DMPChartTab';
import DMPHistoryTab from './components/DMPHistoryTab';
import DMPExportTab from './components/DMPExportTab';
import DM2000Page from '../DM2000/DM2000Page';

const SEARCH_TAB_KEY = 'search';
const CURVE_TAB_KEY = 'chart';

function DMPBridgeContent() {
  const { t } = useLang();
  const { isQC } = useAuth();
  const [stations, setStations] = useState([]);
  const [selectedStationId, setSelectedStationId] = useState(undefined);
  const [stationError, setStationError] = useState('');
  const [selection, setSelection] = useState(null);
  const [activeTab, setActiveTab] = useState(SEARCH_TAB_KEY);

  useEffect(() => {
    let mounted = true;

    const loadStations = async () => {
      try {
        const result = await fetchStations();
        if (!mounted) return;
        setStations(result || []);
        const online = (result || []).filter((station) => station.online);
        setSelectedStationId((prev) => prev ?? online[0]?.id);
      } catch (err) {
        if (!mounted) return;
        setStationError(err.message || 'Failed to load stations');
      }
    };

    loadStations();
    const pollId = setInterval(loadStations, 30000);
    return () => {
      mounted = false;
      clearInterval(pollId);
    };
  }, []);

  useEffect(() => {
    setSelection(null);
  }, [selectedStationId]);

  const handleSelectBatch = (record) => {
    setSelection(record);
    if (record?.id) {
      setActiveTab(isQC ? 'export' : CURVE_TAB_KEY);
    }
  };

  const onlineStations = useMemo(() => stations.filter((station) => station.online), [stations]);
  const selectedStation = useMemo(
    () => stations.find((station) => station.id === selectedStationId) || null,
    [stations, selectedStationId],
  );

  return (
    <div style={{ background: '#fff', minHeight: 'calc(100vh - 112px)', padding: '0 16px' }}>
      <Typography.Title level={4}>{t('dmpBridgeTitle')}</Typography.Title>

      {stationError && <Alert type="error" message={stationError} showIcon style={{ marginBottom: 12 }} />}

      <Card size="small" style={{ marginBottom: 12 }}>
        <div style={{ display: 'flex', gap: 12, alignItems: 'center', flexWrap: 'wrap' }}>
          <Typography.Text strong>{t('dmpStation')}:</Typography.Text>
          <Select
            style={{ minWidth: 280 }}
            placeholder={t('dmpSelectStation')}
            value={selectedStationId}
            onChange={setSelectedStationId}
            options={onlineStations.map((station) => ({
              value: station.id,
              label: station.name,
            }))}
          />
          {selectedStation && (
            <Tag color={selectedStation.online ? 'green' : 'red'}>
              {selectedStation.name} • {selectedStation.online ? t('dmpOnline') : t('dmpOffline')}
            </Tag>
          )}
        </div>
        {onlineStations.length === 0 && (
          <Alert
            style={{ marginTop: 12 }}
            type="warning"
            showIcon
            message={t('dmpNoStations')}
          />
        )}
      </Card>

      {selection && (
        <Card size="small" style={{ marginBottom: 12 }}>
          <Typography.Text strong>{selection.id || '-'}</Typography.Text>
          <Typography.Text type="secondary"> • {selection.dcxh || '-'}</Typography.Text>
          <Typography.Text type="secondary"> • {selection.fdrq || '-'}</Typography.Text>
          {selection.fdfs && (
            <Typography.Text type="secondary"> • {selection.fdfs}</Typography.Text>
          )}
        </Card>
      )}

      <Tabs
        activeKey={activeTab}
        onChange={setActiveTab}
        destroyInactiveTabPane
        items={[
          {
            key: SEARCH_TAB_KEY,
            label: t('dmpSearchTab'),
            children: (
              <DMPFilterPanel
                stationId={selectedStationId}
                selectedBatchId={selection?.id}
                onSelect={handleSelectBatch}
              />
            ),
          },
          ...(!isQC ? [
            {
              key: CURVE_TAB_KEY,
              label: t('dmpChartTab'),
              children: (
                <DMPChartTab
                  stationId={selectedStationId}
                  selection={selection}
                />
              ),
            },
            {
              key: 'data',
              label: t('dmpDataTab'),
              children: (
                <DMPHistoryTab
                  stationId={selectedStationId}
                  selection={selection}
                />
              ),
            },
          ] : []),
          {
            key: 'export',
            label: t('dmpExportTab'),
            children: (
              <DMPExportTab
                stationId={selectedStationId}
                selection={selection}
              />
            ),
          },
        ]}
      />
    </div>
  );
}

export default function BatteryDMPPage() {
  const { t } = useLang();
  const [activeMainTab, setActiveMainTab] = useState('dmp');

  return (
    <div style={{ background: '#fff', minHeight: 'calc(100vh - 112px)' }}>
      <Tabs
        activeKey={activeMainTab}
        onChange={setActiveMainTab}
        destroyInactiveTabPane
        size="large"
        style={{ padding: '0 16px' }}
        items={[
          {
            key: 'dmp',
            label: (
              <span>
                <ExperimentOutlined style={{ marginRight: 6 }} />
                {t('dmpBridgeTitle')}
              </span>
            ),
            children: <DMPBridgeContent />,
          },
          {
            key: 'dm2000',
            label: (
              <span>
                <DatabaseOutlined style={{ marginRight: 6 }} />
                {t('dm2000Title')}
              </span>
            ),
            children: <DM2000Page />,
          },
        ]}
      />
    </div>
  );
}
