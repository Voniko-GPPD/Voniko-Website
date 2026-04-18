import React, { useEffect, useMemo, useState } from 'react';
import { Breadcrumb, Card, Layout, Tabs, Tag, Typography } from 'antd';
import DMPSidebar from './components/DMPSidebar';
import DMPChartTab from './components/DMPChartTab';
import DMPHistoryTab from './components/DMPHistoryTab';
import DMPExportTab from './components/DMPExportTab';

const { Sider, Content } = Layout;

export default function BatteryDMPPage() {
  const [selection, setSelection] = useState(null);
  const [activeTab, setActiveTab] = useState('chart');

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
      <Sider width={280} style={{ background: '#fff', borderRight: '1px solid #f0f0f0', padding: 12 }}>
        <Typography.Title level={5} style={{ marginTop: 0 }}>DMP Database</Typography.Title>
        <DMPSidebar onSelect={setSelection} />
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
              </div>
            )}
          </div>
        </Card>

        <Tabs
          activeKey={activeTab}
          onChange={setActiveTab}
          items={[
            { key: 'chart', label: 'Chart', children: <DMPChartTab selection={selection} /> },
            { key: 'history', label: 'History Data', children: <DMPHistoryTab selection={selection} /> },
            { key: 'export', label: 'Export Reports', children: <DMPExportTab selection={selection} /> },
          ]}
        />
      </Content>
    </Layout>
  );
}
