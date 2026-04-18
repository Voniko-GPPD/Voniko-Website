import React, { useEffect, useMemo, useState } from 'react';
import {
  Alert,
  Badge,
  Breadcrumb,
  Card,
  Layout,
  Select,
  Tabs,
  Tag,
  Typography,
} from 'antd';
import { fetchStations } from '../../api/dmpApi';
import DMPSidebar from './components/DMPSidebar';
import DMPChartTab from './components/DMPChartTab';
import DMPHistoryTab from './components/DMPHistoryTab';
import DMPExportTab from './components/DMPExportTab';

const { Sider, Content } = Layout;

export default function BatteryDMPPage() {
  const [stations, setStations] = useState([]);
  const [selectedStationId, setSelectedStationId] = useState(undefined);
  const [stationError, setStationError] = useState('');
  const [selection, setSelection] = useState(null);
  const [activeTab, setActiveTab] = useState('chart');

  useEffect(() => {
    let mounted = true;
    fetchStations()
      .then((result) => {
        if (!mounted) return;
        setStations(result || []);
        const online = (result || []).filter((station) => station.online);
        setSelectedStationId(online[0]?.id);
      })
      .catch((err) => {
        if (!mounted) return;
        setStationError(err.message || 'Failed to load stations');
      });

    return () => {
      mounted = false;
    };
  }, []);

  const onlineStations = useMemo(() => stations.filter((station) => station.online), [stations]);
  const selectedStation = useMemo(
    () => stations.find((station) => station.id === selectedStationId) || null,
    [stations, selectedStationId]
  );

  useEffect(() => {
    setSelection(null);
  }, [selectedStationId]);

  useEffect(() => {
    setActiveTab('chart');
  }, [selection]);

  const breadcrumbItems = useMemo(() => ([
    { title: `Model: ${selection?.model || '-'}` },
    { title: `Date: ${selection?.date || '-'}` },
    { title: `Batch: ${selection?.batchId || '-'}` },
    { title: `Channel: ${selection?.channel ?? '-'}` },
  ]), [selection]);

  return (
    <Layout style={{ background: '#fff', minHeight: 'calc(100vh - 112px)' }}>
      <Content style={{ padding: '0 16px' }}>
        <Typography.Title level={4}>DMP Battery Data Bridge</Typography.Title>

        {stationError && <Alert type="error" message={stationError} showIcon style={{ marginBottom: 12 }} />}

        <Card size="small" style={{ marginBottom: 12 }}>
          <div style={{ display: 'flex', gap: 12, alignItems: 'center', flexWrap: 'wrap' }}>
            <Typography.Text strong>Station:</Typography.Text>
            <Select
              style={{ minWidth: 280 }}
              placeholder="Select online DMP station"
              value={selectedStationId}
              onChange={setSelectedStationId}
              options={onlineStations.map((station) => ({
                value: station.id,
                label: station.name,
              }))}
            />
            {selectedStation && (
              <Tag color={selectedStation.online ? 'green' : 'red'}>
                {selectedStation.name} • {selectedStation.online ? 'online' : 'offline'}
              </Tag>
            )}
          </div>
          {onlineStations.length === 0 && (
            <Alert
              style={{ marginTop: 12 }}
              type="warning"
              showIcon
              message="No online DMP stations found. Run hardware-services/dmp_service.py on the DMP client machine."
            />
          )}
        </Card>

        <Layout style={{ background: '#fff' }}>
          <Sider width={280} style={{ background: '#fff', borderRight: '1px solid #f0f0f0', padding: 12 }}>
            <Typography.Title level={5} style={{ marginTop: 0 }}>DMP Database</Typography.Title>
            <DMPSidebar
              stationId={selectedStationId}
              onSelect={setSelection}
            />
          </Sider>

          <Content style={{ padding: '0 16px' }}>
            <Card size="small" style={{ marginBottom: 12 }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 12, flexWrap: 'wrap' }}>
                <Breadcrumb items={breadcrumbItems} />
                {selection && (
                  <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
                    <Tag color="blue">{selection.model}</Tag>
                    <Tag color="purple">{selection.date}</Tag>
                    <Tag color="cyan">Batch {selection.batchId}</Tag>
                    <Tag color="green">CH {selection.channel}</Tag>
                    <Badge status={selectedStation?.online ? 'success' : 'error'} text={selectedStation?.name || 'No station'} />
                  </div>
                )}
              </div>
            </Card>

            <Tabs
              activeKey={activeTab}
              onChange={setActiveTab}
              items={[
                { key: 'chart', label: 'Chart', children: <DMPChartTab stationId={selectedStationId} selection={selection} /> },
                { key: 'history', label: 'History Data', children: <DMPHistoryTab stationId={selectedStationId} selection={selection} /> },
                { key: 'export', label: 'Export Reports', children: <DMPExportTab stationId={selectedStationId} selection={selection} /> },
              ]}
            />
          </Content>
        </Layout>
      </Content>
    </Layout>
  );
}
