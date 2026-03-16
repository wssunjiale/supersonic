import type { ActionType, ProColumns } from '@ant-design/pro-components';
import { ProTable } from '@ant-design/pro-components';
import { Button, Input, message, Popconfirm, Select, Space } from 'antd';
import React, { useEffect, useMemo, useRef, useState } from 'react';
import dayjs from 'dayjs';
import TableHeaderFilter from '@/components/TableHeaderFilter';
import { isArrayOfValues } from '@/utils/utils';
import { ISemantic } from '../SemanticModel/data';
import styles from '../SemanticModel/components/style.less';
import {
  batchSyncSupersetDatasets,
  batchDeleteSupersetDataset,
  deleteSupersetDataset,
  getDatabaseList,
  querySupersetDatasets,
  syncSupersetDataset,
  syncSupersetDatasets,
} from '../SemanticModel/service';

const datasetTypeOptions = [
  { label: '物理', value: 'PHYSICAL' },
  { label: '虚拟', value: 'VIRTUAL' },
];

const syncStateOptions = [
  { label: '待同步', value: 'PENDING' },
  { label: '同步失败', value: 'FAILED' },
  { label: '已同步', value: 'SUCCESS' },
];

const SupersetDatasetPage: React.FC = () => {
  const actionRef = useRef<ActionType>();
  const [filters, setFilters] = useState<{
    datasetName?: string;
    datasetType?: string;
    databaseId?: number;
    sourceType?: string;
    needSync?: boolean;
    syncState?: string;
  }>({});
  const [selectedRowKeys, setSelectedRowKeys] = useState<number[]>([]);
  const [databaseList, setDatabaseList] = useState<ISemantic.IDatabaseItem[]>([]);

  useEffect(() => {
    const loadDatabases = async () => {
      const { code, data, msg } = await getDatabaseList();
      if (code === 200) {
        setDatabaseList(data || []);
      } else {
        message.error(msg);
      }
    };
    loadDatabases();
  }, []);

  const databaseMap = useMemo(() => {
    return new Map(databaseList.map((item) => [item.id, item]));
  }, [databaseList]);

  const databaseOptions = useMemo(() => {
    return databaseList.map((item) => ({
      label: item.name,
      value: item.id,
    }));
  }, [databaseList]);

  const deleteSingle = async (id: number) => {
    const { code, msg } = await deleteSupersetDataset(id);
    if (code === 200) {
      message.success('已删除');
      actionRef.current?.reload();
    } else {
      message.error(msg);
    }
  };

  const deleteBatch = async (ids: number[]) => {
    const { code, msg } = await batchDeleteSupersetDataset(ids);
    if (code === 200) {
      message.success('已删除');
      setSelectedRowKeys([]);
      actionRef.current?.reload();
    } else {
      message.error(msg);
    }
  };

  const syncSingle = async (id: number) => {
    const { code, msg } = await syncSupersetDataset(id);
    if (code === 200) {
      message.success('已触发同步');
      actionRef.current?.reload();
    } else {
      message.error(msg);
    }
  };

  const syncBatch = async (ids: number[]) => {
    const { code, msg } = await batchSyncSupersetDatasets(ids);
    if (code === 200) {
      message.success('已触发同步');
      setSelectedRowKeys([]);
      actionRef.current?.reload();
    } else {
      message.error(msg);
    }
  };

  const syncAll = async () => {
    const { code, msg } = await syncSupersetDatasets();
    if (code === 200) {
      message.success('已触发全量同步');
      actionRef.current?.reload();
    } else {
      message.error(msg);
    }
  };

  const columns: ProColumns<ISemantic.ISupersetDatasetItem>[] = [
    {
      dataIndex: 'id',
      title: 'ID',
      width: 80,
    },
    {
      dataIndex: 'datasetName',
      title: '数据集名称',
      ellipsis: true,
    },
    {
      dataIndex: 'datasetType',
      title: '类型',
      width: 90,
      render: (value) => {
        if (value === 'PHYSICAL') {
          return '物理';
        }
        if (value === 'VIRTUAL') {
          return '虚拟';
        }
        return value || '-';
      },
    },
    {
      dataIndex: 'databaseId',
      title: '数据库',
      width: 140,
      render: (value) => {
        if (value === undefined || value === null) {
          return '-';
        }
        const db = databaseMap.get(Number(value));
        return db?.name || value || '-';
      },
    },
    {
      dataIndex: 'schemaName',
      title: 'Schema',
      width: 120,
      ellipsis: true,
    },
    {
      dataIndex: 'tableName',
      title: '表名',
      width: 160,
      ellipsis: true,
    },
    {
      dataIndex: 'dataSetId',
      title: '语义数据集ID',
      width: 120,
    },
    {
      dataIndex: 'syncState',
      title: '同步状态',
      width: 110,
      render: (value) => {
        if (value === 'PENDING') {
          return '待同步';
        }
        if (value === 'FAILED') {
          return '失败';
        }
        if (value === 'SUCCESS') {
          return '已同步';
        }
        return value || '-';
      },
    },
    {
      dataIndex: 'syncErrorMsg',
      title: '失败原因',
      width: 240,
      ellipsis: true,
      render: (value) => value || '-',
    },
    {
      dataIndex: 'nextRetryAt',
      title: '下次重试',
      width: 160,
      render: (value: any) => {
        return value && value !== '-' ? dayjs(value).format('YYYY-MM-DD HH:mm:ss') : '-';
      },
    },
    {
      dataIndex: 'supersetDatasetId',
      title: 'Superset ID',
      width: 120,
    },
    {
      dataIndex: 'syncedAt',
      title: '同步时间',
      width: 160,
      render: (value: any) => {
        return value && value !== '-' ? dayjs(value).format('YYYY-MM-DD HH:mm:ss') : '-';
      },
    },
    {
      dataIndex: 'updatedAt',
      title: '更新时间',
      width: 160,
      render: (value: any) => {
        return value && value !== '-' ? dayjs(value).format('YYYY-MM-DD HH:mm:ss') : '-';
      },
    },
    {
      dataIndex: 'createdBy',
      title: '创建人',
      width: 120,
    },
    {
      title: '操作',
      dataIndex: 'x',
      valueType: 'option',
      width: 180,
      render: (_, record) => {
        return (
          <Space className={styles.ctrlBtnContainer}>
            <Button type="link" onClick={() => syncSingle(record.id)}>
              同步
            </Button>
            <Popconfirm
              title="确认删除该数据集？"
              okText="是"
              cancelText="否"
              onConfirm={() => deleteSingle(record.id)}
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
      <ProTable<ISemantic.ISupersetDatasetItem>
        className={`${styles.classTable} ${styles.disabledSearchTable}`}
        actionRef={actionRef}
        rowKey="id"
        columns={columns}
        search={false}
        options={{ reload: false, density: false, fullScreen: false }}
        pagination={{ pageSize: 10 }}
        tableAlertRender={() => false}
        rowSelection={{
          type: 'checkbox',
          selectedRowKeys,
          onChange: (keys) => setSelectedRowKeys(keys as number[]),
        }}
        request={async (params) => {
          const { current, pageSize } = params;
          const { code, data, msg } = await querySupersetDatasets({
            sourceType: 'SEMANTIC_DATASET',
            needSync: true,
            ...filters,
            current,
            pageSize,
          });
          if (code !== 200) {
            message.error(msg);
            return { data: [], success: false, total: 0 };
          }
          return {
            data: data?.list || [],
            success: true,
            total: data?.total || 0,
          };
        }}
        headerTitle={
          <TableHeaderFilter
            components={[
              {
                label: '名称',
                component: (
                  <Input
                    style={{ width: 200 }}
                    placeholder="请输入名称"
                    allowClear
                    value={filters.datasetName}
                    onChange={(e) => {
                      setFilters({ ...filters, datasetName: e.target.value });
                    }}
                  />
                ),
              },
              {
                label: '类型',
                component: (
                  <Select
                    style={{ width: 160 }}
                    placeholder="请选择类型"
                    allowClear
                    options={datasetTypeOptions}
                    value={filters.datasetType}
                    onChange={(value) => {
                      setFilters({ ...filters, datasetType: value });
                    }}
                  />
                ),
              },
              {
                label: '数据库',
                component: (
                  <Select
                    style={{ width: 200 }}
                    placeholder="请选择数据库"
                    allowClear
                    options={databaseOptions}
                    value={filters.databaseId}
                    onChange={(value) => {
                      setFilters({ ...filters, databaseId: value });
                    }}
                  />
                ),
              },
              {
                label: '同步状态',
                component: (
                  <Select
                    style={{ width: 160 }}
                    placeholder="请选择状态"
                    allowClear
                    options={syncStateOptions}
                    value={filters.syncState}
                    onChange={(value) => {
                      setFilters({ ...filters, syncState: value });
                    }}
                  />
                ),
              },
            ]}
          />
        }
        toolBarRender={() => [
          <Button
            key="syncAll"
            type="primary"
            onClick={() => {
              syncAll();
            }}
          >
            全量同步
          </Button>,
          <Popconfirm
            key="batchSync"
            title="确认同步所选数据集？"
            okText="是"
            cancelText="否"
            onConfirm={() => syncBatch(selectedRowKeys)}
            disabled={!isArrayOfValues(selectedRowKeys)}
          >
            <Button type="primary" disabled={!isArrayOfValues(selectedRowKeys)}>
              批量同步
            </Button>
          </Popconfirm>,
          <Button
            key="reset"
            onClick={() => {
              setFilters({});
              setTimeout(() => {
                actionRef.current?.reload();
              }, 0);
            }}
          >
            重置
          </Button>,
          <Button
            key="search"
            type="primary"
            onClick={() => {
              actionRef.current?.reload();
            }}
          >
            查询
          </Button>,
          <Popconfirm
            key="batchDelete"
            title="确认批量删除所选数据集？"
            okText="是"
            cancelText="否"
            onConfirm={() => deleteBatch(selectedRowKeys)}
            disabled={!isArrayOfValues(selectedRowKeys)}
          >
            <Button type="primary" disabled={!isArrayOfValues(selectedRowKeys)}>
              批量删除
            </Button>
          </Popconfirm>,
        ]}
      />
    </div>
  );
};

export default SupersetDatasetPage;
