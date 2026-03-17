import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import SupersetChart from './index';

jest.mock('@superset-ui/embedded-sdk', () => ({
  embedDashboard: jest.fn(),
}));

jest.mock('../../../service', () => ({
  fetchSupersetGuestToken: jest.fn(),
  fetchSupersetManualDashboards: jest.fn(),
  createSupersetDashboard: jest.fn(),
  pushSupersetChartToDashboard: jest.fn(),
}));

const buildData = (response: any) =>
  ({
    response,
    queryMode: 'SUPERSET',
    queryState: 'SUCCESS',
    queryColumns: [],
    queryResults: [],
  } as any);

const ensureEmbedDashboardMock = () => {
  const { embedDashboard } = require('@superset-ui/embedded-sdk');
  embedDashboard.mockResolvedValue({
    unmount: jest.fn(),
    setThemeMode: jest.fn(),
    setThemeConfig: jest.fn(),
  });
  return embedDashboard;
};

const ensureServiceMocks = () => {
  const {
    fetchSupersetGuestToken,
    fetchSupersetManualDashboards,
    createSupersetDashboard,
    pushSupersetChartToDashboard,
  } = require('../../../service');
  fetchSupersetGuestToken.mockResolvedValue({ data: { token: 'token-default' } });
  fetchSupersetManualDashboards.mockResolvedValue({
    code: 200,
    data: {
      pluginId: 1,
      supersetDomain: 'https://superset.example.com',
      dashboards: [
        {
          id: 9001,
          title: '经营分析总览',
          embeddedId: 'manual-dashboard-9001',
          supersetDomain: 'https://superset.example.com',
        },
      ],
    },
  });
  createSupersetDashboard.mockResolvedValue({
    code: 200,
    data: {
      id: 9101,
      title: '新建看板',
      embeddedId: 'manual-dashboard-9101',
      supersetDomain: 'https://superset.example.com',
    },
  });
  pushSupersetChartToDashboard.mockResolvedValue({ code: 200, data: true });
};

describe('SupersetChart', () => {
  beforeEach(() => {
    document.documentElement.removeAttribute('data-theme');
    document.documentElement.className = '';
    document.documentElement.style.cssText = '';
    document.body.className = '';
    document.body.style.cssText = '';
    ensureEmbedDashboardMock();
    ensureServiceMocks();
  });

  test('does not use embedded url fallback', async () => {
    const data = buildData({
      webPage: { url: 'https://superset.example.com/superset/embedded/uuid-123/', params: [] },
      pluginId: 1,
      chartUuid: 'uuid-123',
      guestToken: 'token-123',
    });
    const { embedDashboard } = require('@superset-ui/embedded-sdk');
    render(<SupersetChart id={1} data={data} />);
    await waitFor(() => {
      expect(screen.getByText('Superset 嵌入信息缺失，无法渲染看板。')).toBeTruthy();
    });
    expect(embedDashboard).not.toHaveBeenCalled();
  });

  test('uses embedded sdk when response provides embed info', async () => {
    const data = buildData({
      webPage: { url: '', params: [] },
      pluginId: 1,
      embeddedId: 'uuid-456',
      supersetDomain: 'https://superset.example.com',
    });
    const { embedDashboard } = require('@superset-ui/embedded-sdk');
    render(<SupersetChart id={1} data={data} />);
    await waitFor(() => {
      expect(embedDashboard).toHaveBeenCalled();
    });
    const args = embedDashboard.mock.calls[0][0];
    expect(args.id).toBe('uuid-456');
    expect(args.supersetDomain).toBe('https://superset.example.com');
    expect(args.iframeTitle).toBe('supersetIframe');
    expect(args.dashboardUiConfig.hideChartControls).toBe(false);
    await expect(args.fetchGuestToken()).resolves.toBe('token-default');
  });

  test('uses first candidate embed as default view and renders switcher', async () => {
    const { embedDashboard } = require('@superset-ui/embedded-sdk');
    embedDashboard.mockClear();
    const data = buildData({
      webPage: { url: '', params: [] },
      pluginId: 1,
      dashboardId: 88,
      dashboardTitle: '分析看板',
      embeddedId: 'embed-line',
      supersetDomain: 'https://superset.example.com',
      vizTypeCandidates: [
        {
          vizType: 'echarts_timeseries_line',
          vizName: 'Line Chart',
          embeddedId: 'embed-line',
          supersetDomain: 'https://superset.example.com',
          chartId: 11,
        },
        {
          vizType: 'echarts_timeseries_bar',
          vizName: 'Bar Chart',
          embeddedId: 'embed-bar',
          supersetDomain: 'https://superset.example.com',
          chartId: 22,
        },
        {
          vizType: 'table',
          vizName: 'Table',
          embeddedId: 'embed-table',
          supersetDomain: 'https://superset.example.com',
          chartId: 33,
        },
      ],
    });
    render(<SupersetChart id={1} data={data} />);
    await waitFor(() => {
      expect(embedDashboard).toHaveBeenCalled();
    });
    expect(embedDashboard).toHaveBeenCalledTimes(1);
    const args = embedDashboard.mock.calls[0][0];
    expect(args.id).toBe('embed-line');
    await expect(args.fetchGuestToken()).resolves.toBe('token-default');
    expect(screen.queryByText('分析看板')).toBeNull();
    expect(screen.getByText('折线图')).toBeTruthy();
    expect(screen.getByText('柱状图')).toBeTruthy();
    expect(screen.getByText('数据表')).toBeTruthy();
    expect(screen.queryByText('Line Chart')).toBeNull();
    expect(screen.getByRole('button', { name: '折线图' })).toBeTruthy();
    expect(screen.getByText('推送到看板')).toBeTruthy();
  });

  test('prefers final dashboard embed when candidates only describe child charts', async () => {
    const { embedDashboard } = require('@superset-ui/embedded-sdk');
    embedDashboard.mockClear();
    const data = buildData({
      webPage: { url: '', params: [] },
      pluginId: 1,
      dashboardId: 88,
      dashboardTitle: '访问趋势分析',
      embeddedId: 'final-dashboard-embed',
      supersetDomain: 'https://superset.example.com',
      vizTypeCandidates: [
        {
          vizType: 'echarts_timeseries_line',
          vizName: '趋势折线图',
          chartId: 11,
          chartUuid: 'chart-uuid-11',
        },
        {
          vizType: 'pie',
          vizName: '占比饼图',
          chartId: 22,
          chartUuid: 'chart-uuid-22',
        },
      ],
    });
    render(<SupersetChart id={1} data={data} />);
    await waitFor(() => {
      expect(embedDashboard).toHaveBeenCalled();
    });
    expect(embedDashboard).toHaveBeenCalledTimes(1);
    expect(embedDashboard.mock.calls[0][0].id).toBe('final-dashboard-embed');
    expect(screen.queryByText('访问趋势分析')).toBeNull();
    expect(screen.getByText('折线图')).toBeTruthy();
    expect(screen.getByText('饼图')).toBeTruthy();
    expect(screen.queryByText('推送到看板')).toBeNull();
  });

  test('switches embedded dashboard when user selects another viz type', async () => {
    const { embedDashboard } = require('@superset-ui/embedded-sdk');
    embedDashboard.mockClear();
    const data = buildData({
      webPage: { url: '', params: [] },
      pluginId: 1,
      dashboardId: 88,
      dashboardTitle: '访问趋势分析',
      embeddedId: 'embed-line',
      supersetDomain: 'https://superset.example.com',
      vizTypeCandidates: [
        {
          vizType: 'echarts_timeseries_line',
          vizName: 'Line Chart',
          embeddedId: 'embed-line',
          supersetDomain: 'https://superset.example.com',
          chartId: 11,
        },
        {
          vizType: 'echarts_timeseries_bar',
          vizName: 'Bar Chart',
          embeddedId: 'embed-bar',
          supersetDomain: 'https://superset.example.com',
          chartId: 22,
        },
        {
          vizType: 'table',
          vizName: 'Table',
          embeddedId: 'embed-table',
          supersetDomain: 'https://superset.example.com',
          chartId: 33,
        },
      ],
    });
    render(<SupersetChart id={1} data={data} />);
    await waitFor(() => {
      expect(embedDashboard).toHaveBeenCalledTimes(1);
    });
    fireEvent.click(screen.getByRole('button', { name: '柱状图' }));
    await waitFor(() => {
      expect(embedDashboard).toHaveBeenCalledTimes(2);
    });
    expect(embedDashboard.mock.calls[1][0].id).toBe('embed-bar');

    fireEvent.click(screen.getByRole('button', { name: '数据表' }));
    await waitFor(() => {
      expect(embedDashboard).toHaveBeenCalledTimes(3);
    });
    expect(embedDashboard.mock.calls[2][0].id).toBe('embed-table');
  });

  test('fetches guest token from response wrapper', async () => {
    const { embedDashboard } = require('@superset-ui/embedded-sdk');
    const { fetchSupersetGuestToken } = require('../../../service');
    fetchSupersetGuestToken.mockResolvedValue({ data: { token: 'token-789' } });
    const data = buildData({
      webPage: { url: '', params: [] },
      pluginId: 1,
      embeddedId: 'uuid-789',
      supersetDomain: 'https://superset.example.com',
    });
    render(<SupersetChart id={1} data={data} />);
    await waitFor(() => {
      expect(embedDashboard).toHaveBeenCalled();
    });
    const args = embedDashboard.mock.calls[0][0];
    await expect(args.fetchGuestToken()).resolves.toBe('token-789');
  });

  test('fetches guest token from direct token response', async () => {
    const { embedDashboard } = require('@superset-ui/embedded-sdk');
    const { fetchSupersetGuestToken } = require('../../../service');
    fetchSupersetGuestToken.mockResolvedValue({ token: 'token-555' });
    const data = buildData({
      webPage: { url: '', params: [] },
      pluginId: 1,
      embeddedId: 'uuid-555',
      supersetDomain: 'https://superset.example.com',
    });
    render(<SupersetChart id={1} data={data} />);
    await waitFor(() => {
      expect(embedDashboard).toHaveBeenCalled();
    });
    const args = embedDashboard.mock.calls[0][0];
    await expect(args.fetchGuestToken()).resolves.toBe('token-555');
  });

  test('always fetches guest token from api', async () => {
    const { embedDashboard } = require('@superset-ui/embedded-sdk');
    const { fetchSupersetGuestToken } = require('../../../service');
    fetchSupersetGuestToken.mockResolvedValue({ data: { token: 'token-new' } });
    const data = buildData({
      webPage: { url: '', params: [] },
      pluginId: 1,
      embeddedId: 'uuid-expired',
      supersetDomain: 'https://superset.example.com',
    });
    render(<SupersetChart id={1} data={data} />);
    await waitFor(() => {
      expect(embedDashboard).toHaveBeenCalled();
    });
    const args = embedDashboard.mock.calls[0][0];
    await expect(args.fetchGuestToken()).resolves.toBe('token-new');
  });

  test('syncs host theme mode and color tokens into embedded dashboard', async () => {
    const { embedDashboard } = require('@superset-ui/embedded-sdk');
    document.documentElement.style.setProperty('--tme-primary-color', '#1672fa');
    document.documentElement.style.setProperty('--component-background', '#ffffff');
    document.documentElement.style.setProperty('--body-background', '#f7fafa');
    document.documentElement.style.setProperty('--text-color', '#181a1a');
    document.documentElement.style.setProperty('--text-color-secondary', '#3d4242');
    document.documentElement.style.setProperty('--border-color-base', '#e1e6e6');

    const data = buildData({
      webPage: { url: '', params: [] },
      pluginId: 1,
      embeddedId: 'uuid-theme-light',
      supersetDomain: 'https://superset.example.com',
    });
    render(<SupersetChart id={1} data={data} />);
    await waitFor(() => {
      expect(embedDashboard).toHaveBeenCalled();
    });
    const instance = await embedDashboard.mock.results[0].value;
    await waitFor(() => {
      expect(instance.setThemeMode).toHaveBeenCalledWith('default');
    });
    expect(instance.setThemeConfig).toHaveBeenCalledWith({
      token: {
        colorPrimary: '#1672fa',
        colorBgBase: '#ffffff',
        colorBgLayout: '#f7fafa',
        colorBgContainer: '#ffffff',
        colorTextBase: '#181a1a',
        colorText: '#181a1a',
        colorTextSecondary: '#3d4242',
        colorBorder: '#e1e6e6',
      },
    });
  });

  test('re-syncs embedded dashboard theme when host theme changes', async () => {
    const { embedDashboard } = require('@superset-ui/embedded-sdk');
    document.documentElement.style.setProperty('--component-background', '#ffffff');
    document.documentElement.style.setProperty('--body-background', '#f7fafa');

    const data = buildData({
      webPage: { url: '', params: [] },
      pluginId: 1,
      embeddedId: 'uuid-theme-switch',
      supersetDomain: 'https://superset.example.com',
    });
    render(<SupersetChart id={1} data={data} />);
    await waitFor(() => {
      expect(embedDashboard).toHaveBeenCalled();
    });
    const instance = await embedDashboard.mock.results[0].value;
    await waitFor(() => {
      expect(instance.setThemeMode).toHaveBeenCalledWith('default');
    });

    document.documentElement.setAttribute('data-theme', 'dark');
    document.documentElement.style.setProperty('--component-background', '#101014');
    document.documentElement.style.setProperty('--body-background', '#0b0c0f');

    await waitFor(() => {
      expect(instance.setThemeMode).toHaveBeenLastCalledWith('dark');
    });
    expect(instance.setThemeConfig).toHaveBeenLastCalledWith(
      expect.objectContaining({
        token: expect.objectContaining({
          colorBgBase: '#101014',
          colorBgLayout: '#0b0c0f',
        }),
      })
    );
  });

  test('shows error when embed info missing', async () => {
    const data = buildData({
      webPage: { url: '', params: [] },
      guestToken: 'token-123',
    });
    render(<SupersetChart id={1} data={data} />);
    await waitFor(() => {
      expect(screen.getByText('Superset 嵌入信息缺失，无法渲染看板。')).toBeTruthy();
    });
  });

  test('does not fetch dashboard list or show push action for dashboard-first response', async () => {
    const data = buildData({
      webPage: { url: '', params: [] },
      pluginId: 1,
      dashboardId: 100,
      embeddedId: 'dashboard-embed-100',
      supersetDomain: 'https://superset.example.com',
    });
    render(<SupersetChart id={1} data={data} />);
    await waitFor(() => {
      expect(screen.queryByText('推送到看板')).toBeNull();
    });
  });

  test('pushes the currently selected chart into existing manual dashboard', async () => {
    const { embedDashboard } = require('@superset-ui/embedded-sdk');
    const { fetchSupersetManualDashboards, pushSupersetChartToDashboard } = require('../../../service');
    embedDashboard.mockClear();
    const data = buildData({
      webPage: { url: '', params: [] },
      pluginId: 1,
      dashboardId: 88,
      dashboardTitle: '访问趋势分析',
      embeddedId: 'embed-line',
      supersetDomain: 'https://superset.example.com',
      vizTypeCandidates: [
        {
          vizType: 'echarts_timeseries_line',
          vizName: 'Line Chart',
          embeddedId: 'embed-line',
          supersetDomain: 'https://superset.example.com',
          chartId: 11,
        },
        {
          vizType: 'echarts_timeseries_bar',
          vizName: 'Bar Chart',
          embeddedId: 'embed-bar',
          supersetDomain: 'https://superset.example.com',
          chartId: 22,
        },
      ],
    });
    render(<SupersetChart id={1} data={data} />);
    await waitFor(() => {
      expect(embedDashboard).toHaveBeenCalledTimes(1);
    });
    fireEvent.click(screen.getByRole('button', { name: '柱状图' }));
    await waitFor(() => {
      expect(embedDashboard).toHaveBeenCalledTimes(2);
    });
    fireEvent.click(screen.getByRole('button', { name: '推送到看板' }));
    await waitFor(() => {
      expect(fetchSupersetManualDashboards).toHaveBeenCalledWith(1);
    });
    await screen.findByText('经营分析总览');
    fireEvent.click(screen.getByRole('button', { name: '经营分析总览' }));
    fireEvent.click(screen.getByRole('button', { name: '推送到所选看板' }));
    await waitFor(() => {
      expect(pushSupersetChartToDashboard).toHaveBeenCalledWith({
        pluginId: 1,
        dashboardId: 9001,
        chartId: 22,
      });
    });
  });

  test('creates a new dashboard and pushes current chart when requested', async () => {
    const { embedDashboard } = require('@superset-ui/embedded-sdk');
    const { createSupersetDashboard, pushSupersetChartToDashboard } = require('../../../service');
    embedDashboard.mockClear();
    const data = buildData({
      webPage: { url: '', params: [] },
      pluginId: 1,
      dashboardId: 88,
      dashboardTitle: '访问趋势分析',
      embeddedId: 'embed-line',
      supersetDomain: 'https://superset.example.com',
      vizTypeCandidates: [
        {
          vizType: 'echarts_timeseries_line',
          vizName: 'Line Chart',
          embeddedId: 'embed-line',
          supersetDomain: 'https://superset.example.com',
          chartId: 11,
        },
      ],
    });
    render(<SupersetChart id={1} data={data} />);
    await waitFor(() => {
      expect(embedDashboard).toHaveBeenCalledTimes(1);
    });
    fireEvent.click(screen.getByRole('button', { name: '推送到看板' }));
    fireEvent.change(screen.getByPlaceholderText('输入新看板名称'), {
      target: { value: '我的趋势看板' },
    });
    fireEvent.click(screen.getByRole('button', { name: '新建并推送' }));
    await waitFor(() => {
      expect(createSupersetDashboard).toHaveBeenCalledWith({
        pluginId: 1,
        title: '我的趋势看板',
      });
    });
    await waitFor(() => {
      expect(pushSupersetChartToDashboard).toHaveBeenCalledWith({
        pluginId: 1,
        dashboardId: 9101,
        chartId: 11,
      });
    });
  });
});
