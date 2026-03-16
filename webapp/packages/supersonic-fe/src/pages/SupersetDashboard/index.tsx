import type { ActionType, ProColumns } from '@ant-design/pro-components';
import { ProTable } from '@ant-design/pro-components';
import { Button, Input, message, Modal, Popconfirm, Space } from 'antd';
import React, { useMemo, useRef, useState } from 'react';
import styles from '../SemanticModel/components/style.less';
import {
  createSupersetDashboard,
  deleteSupersetDashboard,
  fetchSupersetManualDashboards,
  SupersetDashboardItem,
  SupersetDashboardManageResp,
} from './service';

type ApiEnvelope<T> = {
  code?: number | string;
  msg?: string;
  data?: T;
};

function unwrapApiEnvelope<T>(payload: ApiEnvelope<T> | T | undefined): T | undefined {
  if (payload == null) {
    return undefined;
  }
  if (typeof payload === 'object' && 'code' in (payload as Record<string, unknown>)) {
    const envelope = payload as ApiEnvelope<T>;
    const code = Number(envelope.code);
    if (Number.isFinite(code) && code !== 200 && code !== 0) {
      throw new Error(envelope.msg || '请求失败');
    }
    return envelope.data;
  }
  return payload as T;
}

const SupersetDashboardPage: React.FC = () => {
  const actionRef = useRef<ActionType>();
  const [pluginId, setPluginId] = useState<number | undefined>();
  const [supersetDomain, setSupersetDomain] = useState<string | undefined>();
  const [createOpen, setCreateOpen] = useState(false);
  const [creating, setCreating] = useState(false);
  const [title, setTitle] = useState('');

  const resolveManageResponse = (
    resp: ApiEnvelope<SupersetDashboardManageResp> | SupersetDashboardManageResp | undefined
  ) => {
    const resolved = unwrapApiEnvelope<SupersetDashboardManageResp>(resp);
    const dashboards = resolved?.dashboards || [];
    if (resolved?.pluginId && resolved.pluginId !== pluginId) {
      setPluginId(resolved.pluginId);
    }
    if (resolved?.supersetDomain && resolved.supersetDomain !== supersetDomain) {
      setSupersetDomain(resolved.supersetDomain);
    }
    return dashboards;
  };

  const normalizedDomain = useMemo(() => {
    if (!supersetDomain) {
      return undefined;
    }
    return supersetDomain.endsWith('/') ? supersetDomain.slice(0, -1) : supersetDomain;
  }, [supersetDomain]);

  const handleCreate = async () => {
    if (!title.trim()) {
      message.error('请输入看板名称');
      return;
    }
    setCreating(true);
    try {
      const created = unwrapApiEnvelope<SupersetDashboardItem>(await createSupersetDashboard({
        pluginId,
        title: title.trim(),
      }));
      if (!created?.id) {
        throw new Error('创建失败');
      }
      message.success('已创建');
      setCreateOpen(false);
      setTitle('');
      actionRef.current?.reload();
    } catch (error: any) {
      const msg = error?.message || '创建失败';
      message.error(msg);
    } finally {
      setCreating(false);
    }
  };

  const handleDelete = async (dashboardId: number) => {
    try {
      const deleted = unwrapApiEnvelope<boolean>(await deleteSupersetDashboard({ pluginId, dashboardId }));
      if (deleted !== true) {
        throw new Error('删除失败');
      }
      message.success('已删除');
      actionRef.current?.reload();
    } catch (error: any) {
      const msg = error?.message || '删除失败';
      message.error(msg);
    }
  };

  const handleView = (record: SupersetDashboardItem) => {
    const embeddedId = record.embeddedId;
    const domain = record.supersetDomain || normalizedDomain;
    if (!embeddedId || !domain) {
      message.error('嵌入信息缺失');
      return;
    }
    const query = new URLSearchParams();
    query.set('embeddedId', embeddedId);
    query.set('supersetDomain', domain);
    if (pluginId) {
      query.set('pluginId', String(pluginId));
    }
    if (record.title) {
      query.set('title', record.title);
    }
    window.open(`/webapp/supersetDashboard/embed?${query.toString()}`, '_blank');
  };

  const handleEdit = (record: SupersetDashboardItem) => {
    if (record.editUrl) {
      window.open(record.editUrl, '_blank');
      return;
    }
    const domain = record.supersetDomain || normalizedDomain;
    if (!domain || !record.id) {
      message.error('编辑链接缺失');
      return;
    }
    window.open(`${domain}/superset/dashboard/${record.id}/?edit=true`, '_blank');
  };

  const columns: ProColumns<SupersetDashboardItem>[] = [
    {
      dataIndex: 'id',
      title: 'ID',
      width: 80,
    },
    {
      dataIndex: 'title',
      title: '看板名称',
      ellipsis: true,
    },
    {
      dataIndex: 'embeddedId',
      title: '嵌入ID',
      width: 220,
      ellipsis: true,
    },
    {
      title: '操作',
      dataIndex: 'x',
      valueType: 'option',
      width: 180,
      render: (_, record) => {
        return (
          <Space className={styles.ctrlBtnContainer}>
            <Button type="link" onClick={() => handleView(record)}>
              查看
            </Button>
            <Button type="link" onClick={() => handleEdit(record)}>
              编辑
            </Button>
            <Popconfirm
              title="确认删除该看板？"
              okText="是"
              cancelText="否"
              onConfirm={() => handleDelete(record.id)}
            >
              <Button type="link">删除</Button>
            </Popconfirm>
          </Space>
        );
      },
    },
  ];

  return (
    <div style={{ margin: 20 }}>
      <ProTable<SupersetDashboardItem>
        className={`${styles.classTable} ${styles.disabledSearchTable}`}
        actionRef={actionRef}
        rowKey="id"
        columns={columns}
        search={false}
        options={{ reload: false, density: false, fullScreen: false }}
        pagination={{ pageSize: 10 }}
        tableAlertRender={() => false}
        request={async () => {
          try {
            const resp = await fetchSupersetManualDashboards(pluginId);
            const dashboards = resolveManageResponse(resp);
            return {
              data: dashboards,
              success: true,
              total: dashboards.length,
            };
          } catch (error: any) {
            const msg = error?.message || '获取看板列表失败';
            message.error(msg);
            return { data: [], success: false, total: 0 };
          }
        }}
        toolBarRender={() => [
          <Button
            key="create"
            type="primary"
            onClick={() => {
              setCreateOpen(true);
            }}
          >
            新增看板
          </Button>,
          <Button
            key="refresh"
            onClick={() => {
              actionRef.current?.reload();
            }}
          >
            刷新
          </Button>,
        ]}
      />
      <Modal
        title="新增看板"
        open={createOpen}
        onOk={handleCreate}
        onCancel={() => {
          if (!creating) {
            setCreateOpen(false);
          }
        }}
        confirmLoading={creating}
        okText="创建"
        cancelText="取消"
      >
        <Input
          placeholder="请输入看板名称"
          maxLength={120}
          value={title}
          onChange={(e) => setTitle(e.target.value)}
          allowClear
        />
      </Modal>
    </div>
  );
};

export default SupersetDashboardPage;
