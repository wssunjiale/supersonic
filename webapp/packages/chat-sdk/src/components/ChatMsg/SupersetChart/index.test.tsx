import { fireEvent, render, waitFor } from '@testing-library/react';
import SupersetChart, { filterTemporaryDashboards } from './index';

jest.mock('@superset-ui/embedded-sdk', () => ({
  embedDashboard: jest.fn(),
}));

jest.mock('../../../service', () => ({
  fetchSupersetGuestToken: jest.fn(),
  fetchSupersetDashboards: jest.fn(),
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
  embedDashboard.mockResolvedValue({ unmount: jest.fn() });
  return embedDashboard;
};

const ensureServiceMocks = () => {
  const { fetchSupersetDashboards, fetchSupersetGuestToken } = require('../../../service');
  fetchSupersetDashboards.mockResolvedValue({ data: [] });
  fetchSupersetGuestToken.mockResolvedValue({ data: { token: 'token-default' } });
};

describe('SupersetChart', () => {
  beforeEach(() => {
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
    const { getByText } = render(<SupersetChart id={1} data={data} />);
    await waitFor(() => {
      expect(getByText('Superset 嵌入信息缺失，无法渲染看板。')).toBeTruthy();
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

  test('switches embed when viz type candidates change', async () => {
    const { embedDashboard } = require('@superset-ui/embedded-sdk');
    embedDashboard.mockClear();
    const data = buildData({
      webPage: { url: '', params: [] },
      pluginId: 1,
      vizTypeCandidates: [
        {
          vizType: 'bar',
          vizName: 'Bar Chart',
          embeddedId: 'embed-1',
          supersetDomain: 'https://superset.example.com',
          chartId: 11,
        },
        {
          vizType: 'line',
          vizName: 'Line Chart',
          embeddedId: 'embed-2',
          supersetDomain: 'https://superset.example.com',
          chartId: 22,
        },
      ],
    });
    const { getByText } = render(<SupersetChart id={1} data={data} />);
    await waitFor(() => {
      expect(embedDashboard).toHaveBeenCalled();
    });
    expect(embedDashboard.mock.calls[0][0].id).toBe('embed-1');
    fireEvent.click(getByText('Line Chart'));
    await waitFor(() => {
      expect(embedDashboard).toHaveBeenCalledTimes(2);
    });
    const args = embedDashboard.mock.calls[1][0];
    expect(args.id).toBe('embed-2');
    await expect(args.fetchGuestToken()).resolves.toBe('token-default');
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

  test('shows error when embed info missing', async () => {
    const data = buildData({
      webPage: { url: '', params: [] },
      guestToken: 'token-123',
    });
    const { getByText } = render(<SupersetChart id={1} data={data} />);
    await waitFor(() => {
      expect(getByText('Superset 嵌入信息缺失，无法渲染看板。')).toBeTruthy();
    });
  });

  test('shows push button when dashboards exist', () => {
    const data = buildData({
      webPage: { url: 'https://superset.example.com/embed', params: [] },
      pluginId: 1,
      chartId: 2,
      dashboards: [{ id: 10, title: 'Sales' }],
    });
    const { getByText } = render(<SupersetChart id={1} data={data} />);
    expect(getByText('推送到看板')).toBeTruthy();
  });

  test('filters temporary dashboards by supersonic prefix', () => {
    const dashboards = [
      { id: 10, title: 'supersonic_Plugin_123' },
      { id: 11, title: 'Sales' },
    ];
    expect(filterTemporaryDashboards(dashboards, 'Plugin')).toEqual([{ id: 11, title: 'Sales' }]);
  });
});
