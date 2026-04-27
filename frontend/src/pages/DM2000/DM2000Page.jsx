import React, { useEffect, useMemo, useState } from 'react';
import { Alert, Card, Select, Tabs, Tag, Typography } from 'antd';
import { fetchStations } from '../../api/dmpApi';
import { useLang } from '../../contexts/LangContext';
import { useAuth } from '../../contexts/AuthContext';
import DM2000FilterPanel from './components/DM2000FilterPanel';
import DM2000CurveTab from './components/DM2000CurveTab';
import DM2000DataTab from './components/DM2000DataTab';
import DM2000DailyVoltTab from './components/DM2000DailyVoltTab';
import DM2000ExportTab from './components/DM2000ExportTab';

const SEARCH_TAB_KEY = 'search';
const CURVE_TAB_KEY = 'curve';

export default function DM2000Page() {
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

  const handleSelectArchive = (record) => {
    setSelection(record);
    if (record?.archname) {
      setActiveTab(isQC ? 'export' : CURVE_TAB_KEY);
    }
  };

  const onlineStations = useMemo(() => stations.filter((station) => station.online), [stations]);
  const selectedStation = useMemo(
    () => stations.find((station) => station.id === selectedStationId) || null,
    [stations, selectedStationId]
  );

  return (
    <div style={{ background: '#fff', minHeight: 'calc(100vh - 112px)', padding: '0 16px' }}>
      <Typography.Title level={4}>{t('dm2000Title')}</Typography.Title>

      {stationError && <Alert type="error" message={stationError} showIcon style={{ marginBottom: 12 }} />}

      <Card size="small" style={{ marginBottom: 12 }}>
        <div style={{ display: 'flex', gap: 12, alignItems: 'center', flexWrap: 'wrap' }}>
          <Typography.Text strong>{t('dmpStation')}:</Typography.Text>
          <Select
            style={{ minWidth: 280 }}
            value={selectedStationId}
            placeholder={t('dmpSelectStation')}
            onChange={setSelectedStationId}
            options={onlineStations.map((station) => ({ value: station.id, label: station.name }))}
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
          <Typography.Text strong>{selection.archname || '-'}</Typography.Text>
          <Typography.Text type="secondary"> • {selection.dcxh || '-'}</Typography.Text>
          <Typography.Text type="secondary"> • {selection.startdate || '-'}</Typography.Text>
        </Card>
      )}

      <Tabs
        activeKey={activeTab}
        onChange={setActiveTab}
        destroyInactiveTabPane
        items={[
          {
            key: SEARCH_TAB_KEY,
            label: t('dm2000SearchTab'),
            children: (
              <DM2000FilterPanel
                stationId={selectedStationId}
                selectedArchname={selection?.archname}
                onSelect={handleSelectArchive}
              />
            ),
          },
          ...(!isQC ? [
            {
              key: CURVE_TAB_KEY,
              label: t('dm2000CurveTab'),
              children: (
                <DM2000CurveTab
                  stationId={selectedStationId}
                  selection={selection}
                />
              ),
            },
            {
              key: 'data',
              label: t('dm2000DataTab'),
              children: (
                <DM2000DataTab
                  stationId={selectedStationId}
                  selection={selection}
                />
              ),
            },
            {
              key: 'daily',
              label: t('dm2000DailyVoltTab'),
              children: (
                <DM2000DailyVoltTab
                  stationId={selectedStationId}
                  selection={selection}
                />
              ),
            },
          ] : []),
          {
            key: 'export',
            label: t('dm2000ExportTab'),
            children: (
              <DM2000ExportTab
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
