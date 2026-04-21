import React, { useEffect, useMemo, useState } from 'react';
import { Alert, Card, Layout, Select, Tabs, Tag, Typography } from 'antd';
import { fetchStations } from '../../api/dmpApi';
import { useLang } from '../../contexts/LangContext';
import DM2000FilterPanel from './components/DM2000FilterPanel';
import DM2000CurveTab from './components/DM2000CurveTab';
import DM2000DataTab from './components/DM2000DataTab';
import DM2000DailyVoltTab from './components/DM2000DailyVoltTab';
import DM2000ExportTab from './components/DM2000ExportTab';

const { Sider, Content } = Layout;

export default function DM2000Page() {
  const { t } = useLang();
  const [stations, setStations] = useState([]);
  const [selectedStationId, setSelectedStationId] = useState(undefined);
  const [stationError, setStationError] = useState('');
  const [selection, setSelection] = useState(null);
  const [activeTab, setActiveTab] = useState('curve');
  const [selectedBaty, setSelectedBaty] = useState(0);

  useEffect(() => {
    let mounted = true;

    const loadStations = async () => {
      try {
        const result = await fetchStations();
        if (!mounted) return;
        setStations(result || []);
        const online = (result || []).filter((station) => station.online);
        setSelectedStationId(online[0]?.id);
      } catch (err) {
        if (!mounted) return;
        setStationError(err.message || 'Failed to load stations');
      }
    };

    loadStations();
    return () => {
      mounted = false;
    };
  }, []);

  useEffect(() => {
    setSelection(null);
  }, [selectedStationId]);

  useEffect(() => {
    setSelectedBaty(0);
    setActiveTab('curve');
  }, [selection?.archname]);

  const onlineStations = useMemo(() => stations.filter((station) => station.online), [stations]);
  const selectedStation = useMemo(
    () => stations.find((station) => station.id === selectedStationId) || null,
    [stations, selectedStationId]
  );

  return (
    <Layout style={{ background: '#fff', minHeight: 'calc(100vh - 112px)' }}>
      <Content style={{ padding: '0 16px' }}>
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

        <Layout style={{ background: '#fff' }}>
          <Sider width={360} style={{ background: '#fff', borderRight: '1px solid #f0f0f0', padding: 12 }}>
            <DM2000FilterPanel
              stationId={selectedStationId}
              selectedArchname={selection?.archname}
              onSelect={setSelection}
            />
          </Sider>
          <Content style={{ padding: '0 16px' }}>
            <Tabs
              activeKey={activeTab}
              onChange={setActiveTab}
              destroyInactiveTabPane
              items={[
                {
                  key: 'curve',
                  label: t('dm2000CurveTab'),
                  children: (
                    <DM2000CurveTab
                      stationId={selectedStationId}
                      selection={selection}
                      selectedBaty={selectedBaty}
                      onBatyChange={setSelectedBaty}
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
                      selectedBaty={selectedBaty}
                      onBatyChange={setSelectedBaty}
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
                      selectedBaty={selectedBaty}
                      onBatyChange={setSelectedBaty}
                    />
                  ),
                },
                {
                  key: 'export',
                  label: t('dm2000ExportTab'),
                  children: (
                    <DM2000ExportTab
                      stationId={selectedStationId}
                      selection={selection}
                      selectedBaty={selectedBaty}
                      onBatyChange={setSelectedBaty}
                    />
                  ),
                },
              ]}
            />
          </Content>
        </Layout>
      </Content>
    </Layout>
  );
}
