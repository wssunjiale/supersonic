import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Button, Input, Modal, message } from 'antd';
import { embedDashboard } from '@superset-ui/embedded-sdk';
import type { EmbeddedDashboard, ThemeMode } from '@superset-ui/embedded-sdk';
import {
  MsgDataType,
  SupersetChartResponseType,
  SupersetDashboardItem,
  SupersetDashboardManageResp,
  SupersetVizTypeCandidate,
} from '../../../common/type';
import {
  createSupersetDashboard,
  fetchSupersetGuestToken,
  fetchSupersetManualDashboards,
  pushSupersetChartToDashboard,
} from '../../../service';

type Props = {
  id: string | number;
  data: MsgDataType;
  triggerResize?: boolean;
};

type SupersetChartView = SupersetVizTypeCandidate & {
  key: string;
  label: string;
};

type ApiEnvelope<T> = {
  code?: number | string;
  msg?: string;
  data?: T;
};

const DEFAULT_HEIGHT = 800;
const SUPERSET_IFRAME_TITLE = 'supersetIframe';
const DEFAULT_THEME_MODE = 'default' as ThemeMode;
const DARK_THEME_MODE = 'dark' as ThemeMode;

type EmbedInstance = EmbeddedDashboard;
type SupersetThemeConfig = {
  token?: Record<string, string>;
};

const SUPERSET_VIZTYPE_ZH_LABELS: Record<string, string> = {
  big_number: '指标卡',
  big_number_total: '累计指标卡',
  box_plot: '箱线图',
  bubble: '气泡图',
  bubble_v2: '气泡图',
  bullet: '子弹图',
  cal_heatmap: '日历热力图',
  cartodiagram: '地图叠加图',
  chord: '弦图',
  compare: '百分比变化图',
  country_map: '国家地图',
  dashboard: '看板',
  deck_arc: '弧线地图',
  deck_contour: '等值线地图',
  deck_geojson: 'GeoJSON 地图',
  deck_grid: '网格地图',
  deck_heatmap: '热力地图',
  deck_hex: '六边形地图',
  deck_multi: '复合地图',
  deck_path: '路径地图',
  deck_polygon: '多边形地图',
  deck_scatter: '散点地图',
  deck_screengrid: '屏幕网格地图',
  echarts_area: '面积图',
  echarts_timeseries: '时间序列图',
  echarts_timeseries_bar: '柱状图',
  echarts_timeseries_line: '折线图',
  echarts_timeseries_scatter: '散点图',
  echarts_timeseries_smooth: '平滑折线图',
  echarts_timeseries_step: '阶梯折线图',
  funnel: '漏斗图',
  gantt_chart: '甘特图',
  gauge_chart: '仪表盘',
  graph_chart: '关系图',
  handlebars: '自定义模板',
  heatmap_v2: '热力图',
  histogram_v2: '直方图',
  horizon: '地平线图',
  mapbox: 'MapBox 地图',
  mixed_timeseries: '混合时序图',
  paired_ttest: '配对 T 检验表',
  para: '平行坐标图',
  partition: '分区图',
  pie: '饼图',
  pivot_table_v2: '透视表',
  radar: '雷达图',
  rose: '玫瑰图',
  sankey_v2: '桑基图',
  sunburst_v2: '旭日图',
  table: '数据表',
  time_pivot: '时间透视表',
  time_table: '时间表',
  tree_chart: '树图',
  treemap_v2: '矩形树图',
  waterfall: '瀑布图',
  word_cloud: '词云图',
  world_map: '世界地图',
};

const hasChineseText = (value?: string) => /[\u4e00-\u9fff]/.test(value || '');

function inferVizTypeLabel(value?: string) {
  if (!value) {
    return '';
  }
  const normalized = value.trim().toLowerCase();
  if (!normalized) {
    return '';
  }
  if (normalized.includes('table')) {
    return '数据表';
  }
  if (normalized.includes('line')) {
    return '折线图';
  }
  if (normalized.includes('bar')) {
    return '柱状图';
  }
  if (normalized.includes('pie')) {
    return '饼图';
  }
  if (normalized.includes('area')) {
    return '面积图';
  }
  if (normalized.includes('scatter')) {
    return '散点图';
  }
  if (normalized.includes('radar')) {
    return '雷达图';
  }
  if (normalized.includes('heatmap')) {
    return '热力图';
  }
  if (normalized.includes('funnel')) {
    return '漏斗图';
  }
  if (normalized.includes('gantt')) {
    return '甘特图';
  }
  if (normalized.includes('gauge')) {
    return '仪表盘';
  }
  if (normalized.includes('map')) {
    return '地图';
  }
  if (normalized.includes('treemap')) {
    return '矩形树图';
  }
  if (normalized.includes('sunburst')) {
    return '旭日图';
  }
  if (normalized.includes('sankey')) {
    return '桑基图';
  }
  if (normalized.includes('word')) {
    return '词云图';
  }
  return '';
}

function resolveVizTypeLabel(vizType?: string, vizName?: string, fallbackLabel = '图表') {
  const normalizedVizType = vizType?.trim().toLowerCase();
  const mappedLabel = normalizedVizType ? SUPERSET_VIZTYPE_ZH_LABELS[normalizedVizType] : '';
  if (mappedLabel) {
    return mappedLabel;
  }
  if (hasChineseText(vizName)) {
    return vizName!.trim();
  }
  const inferredLabel = inferVizTypeLabel(vizName) || inferVizTypeLabel(normalizedVizType);
  if (inferredLabel) {
    return inferredLabel;
  }
  return vizName?.trim() || fallbackLabel;
}

function unwrapApiEnvelope<T>(payload: ApiEnvelope<T> | T | null | undefined): T | undefined {
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

function normalizeThemeModeHint(value?: string): ThemeMode | undefined {
  const normalized = value?.trim().toLowerCase();
  if (!normalized) {
    return undefined;
  }
  if (normalized.includes('dark')) {
    return DARK_THEME_MODE;
  }
  if (
    normalized.includes('light') ||
    normalized.includes('default') ||
    normalized.includes('white')
  ) {
    return DEFAULT_THEME_MODE;
  }
  return undefined;
}

function parseBackgroundColor(value?: string): [number, number, number] | null {
  const normalized = value?.trim().toLowerCase();
  if (!normalized || normalized === 'transparent' || normalized === 'inherit') {
    return null;
  }

  const hexMatch = normalized.match(/^#([0-9a-f]{3}|[0-9a-f]{6})$/i);
  if (hexMatch) {
    const hex = hexMatch[1];
    if (hex.length === 3) {
      return [
        parseInt(`${hex[0]}${hex[0]}`, 16),
        parseInt(`${hex[1]}${hex[1]}`, 16),
        parseInt(`${hex[2]}${hex[2]}`, 16),
      ];
    }
    return [
      parseInt(hex.slice(0, 2), 16),
      parseInt(hex.slice(2, 4), 16),
      parseInt(hex.slice(4, 6), 16),
    ];
  }

  const rgbMatch = normalized.match(/^rgba?\(([^)]+)\)$/);
  if (!rgbMatch) {
    return null;
  }
  const channels = rgbMatch[1]
    .split(',')
    .slice(0, 3)
    .map(item => Number(item.trim()));
  if (channels.length !== 3 || channels.some(channel => !Number.isFinite(channel))) {
    return null;
  }
  return [channels[0], channels[1], channels[2]];
}

function inferThemeModeFromBackgroundColor(backgroundColor?: string): ThemeMode | undefined {
  const channels = parseBackgroundColor(backgroundColor);
  if (!channels) {
    return undefined;
  }
  const [red, green, blue] = channels.map(channel => channel / 255);
  const brightness = 0.299 * red + 0.587 * green + 0.114 * blue;
  return brightness < 0.5 ? DARK_THEME_MODE : DEFAULT_THEME_MODE;
}

function resolveHostBackgroundColor(): string | undefined {
  if (typeof window === 'undefined' || typeof document === 'undefined') {
    return undefined;
  }
  const rootStyle = window.getComputedStyle(document.documentElement);
  const bodyStyle = document.body ? window.getComputedStyle(document.body) : undefined;
  const candidates = [
    rootStyle.getPropertyValue('--component-background'),
    rootStyle.getPropertyValue('--body-background'),
    rootStyle.getPropertyValue('--light-background'),
    bodyStyle?.backgroundColor,
    '#ffffff',
  ];
  const resolved = candidates.find(value => value && value.trim())?.trim();
  return resolved || '#ffffff';
}

function readCssTokenValue(style: CSSStyleDeclaration | undefined, name: string): string | undefined {
  const value = style?.getPropertyValue(name)?.trim();
  return value || undefined;
}

function resolveHostThemeConfig(): SupersetThemeConfig | undefined {
  if (typeof window === 'undefined' || typeof document === 'undefined') {
    return undefined;
  }
  const rootStyle = window.getComputedStyle(document.documentElement);
  const bodyStyle = document.body ? window.getComputedStyle(document.body) : undefined;
  const primaryColor =
    readCssTokenValue(rootStyle, '--tme-primary-color') ||
    readCssTokenValue(rootStyle, '--primary-color');
  const backgroundColor =
    readCssTokenValue(rootStyle, '--component-background') ||
    readCssTokenValue(rootStyle, '--body-background') ||
    bodyStyle?.backgroundColor?.trim() ||
    '#ffffff';
  const layoutBackground =
    readCssTokenValue(rootStyle, '--body-background') ||
    readCssTokenValue(rootStyle, '--light-background') ||
    backgroundColor;
  const textColor =
    readCssTokenValue(rootStyle, '--text-color') ||
    bodyStyle?.color?.trim();
  const secondaryTextColor = readCssTokenValue(rootStyle, '--text-color-secondary');
  const borderColor = readCssTokenValue(rootStyle, '--border-color-base');
  const token = Object.fromEntries(
    Object.entries({
      colorPrimary: primaryColor,
      colorBgBase: backgroundColor,
      colorBgLayout: layoutBackground,
      colorBgContainer: backgroundColor,
      colorTextBase: textColor,
      colorText: textColor,
      colorTextSecondary: secondaryTextColor,
      colorBorder: borderColor,
    }).filter(([, value]) => Boolean(value))
  ) as Record<string, string>;
  if (Object.keys(token).length === 0) {
    return undefined;
  }
  return { token };
}

function resolveHostThemeMode(): ThemeMode {
  if (typeof document === 'undefined') {
    return DEFAULT_THEME_MODE;
  }
  const docElement = document.documentElement;
  const explicitMode =
    normalizeThemeModeHint(docElement.getAttribute('data-theme') || undefined) ||
    normalizeThemeModeHint(docElement.dataset.theme) ||
    normalizeThemeModeHint(docElement.className) ||
    normalizeThemeModeHint(document.body?.className);
  if (explicitMode) {
    return explicitMode;
  }

  const inferredMode = inferThemeModeFromBackgroundColor(resolveHostBackgroundColor());
  if (inferredMode) {
    return inferredMode;
  }

  if (typeof window !== 'undefined' && window.matchMedia) {
    return window.matchMedia('(prefers-color-scheme: dark)').matches
      ? DARK_THEME_MODE
      : DEFAULT_THEME_MODE;
  }
  return DEFAULT_THEME_MODE;
}

const SupersetChart: React.FC<Props> = ({ id, data, triggerResize }) => {
  const [height, setHeight] = useState(DEFAULT_HEIGHT);
  const [backgroundColor, setBackgroundColor] = useState<string>();
  const [activeViewKey, setActiveViewKey] = useState('');
  const [pushModalOpen, setPushModalOpen] = useState(false);
  const [pushLoading, setPushLoading] = useState(false);
  const [dashboardLoading, setDashboardLoading] = useState(false);
  const [manualDashboards, setManualDashboards] = useState<SupersetDashboardItem[]>([]);
  const [selectedDashboardId, setSelectedDashboardId] = useState<number>();
  const [newDashboardTitle, setNewDashboardTitle] = useState('');
  const embedContainerRef = useRef<HTMLDivElement>(null);
  const embedInstanceRef = useRef<EmbedInstance | null>(null);
  const backgroundColorRef = useRef<string>();
  const response = data.response as SupersetChartResponseType;
  const webPage = response?.webPage;

  const resolveGuestToken = (payload: any) => {
    const resolvedPayload = unwrapApiEnvelope<any>(payload) ?? payload;
    if (!resolvedPayload) {
      return '';
    }
    if (typeof resolvedPayload === 'string') {
      return resolvedPayload;
    }
    if (typeof resolvedPayload.token === 'string' && resolvedPayload.token) {
      return resolvedPayload.token;
    }
    if (typeof resolvedPayload?.data?.token === 'string' && resolvedPayload.data.token) {
      return resolvedPayload.data.token;
    }
    return '';
  };

  const params = useMemo(() => {
    const rawParams = webPage?.params || webPage?.paramOptions || [];
    return Array.isArray(rawParams) ? rawParams : [];
  }, [webPage]);

  const defaultMinHeight = useMemo(() => {
    const heightValue =
      params?.find((option: any) => option.paramType === 'FORWARD' && option.key === 'height')
        ?.value ?? DEFAULT_HEIGHT;
    const numericValue = typeof heightValue === 'number' ? heightValue : Number(heightValue);
    if (!Number.isFinite(numericValue) || numericValue <= 0) {
      return DEFAULT_HEIGHT;
    }
    return numericValue;
  }, [params]);

  const rawCandidateLabels = useMemo(() => {
    const rawCandidates = Array.isArray(response?.vizTypeCandidates) ? response.vizTypeCandidates : [];
    return rawCandidates
      .map((candidate, index) => ({
        key: `${candidate?.chartId || candidate?.chartUuid || candidate?.vizType || index}`,
        label: resolveVizTypeLabel(
          candidate?.vizType,
          candidate?.vizName,
          `图表${index + 1}`
        ),
      }))
      .filter(item => Boolean(item.label));
  }, [response?.vizTypeCandidates]);

  const interactiveViewCandidates = useMemo<SupersetChartView[]>(() => {
    const rawCandidates = Array.isArray(response?.vizTypeCandidates) ? response.vizTypeCandidates : [];
    return rawCandidates
      .map((candidate, index) => {
        const label = resolveVizTypeLabel(
          candidate?.vizType,
          candidate?.vizName,
          `图表${index + 1}`
        );
        const embeddedId = candidate?.embeddedId;
        const supersetDomain = candidate?.supersetDomain;
        if (!label || !embeddedId || !supersetDomain) {
          return null;
        }
        return {
          ...candidate,
          embeddedId,
          supersetDomain,
          key: `${embeddedId}-${candidate?.chartId || candidate?.chartUuid || candidate?.vizType || index}`,
          label,
        } as SupersetChartView;
      })
      .filter(Boolean) as SupersetChartView[];
  }, [response?.vizTypeCandidates]);

  const viewCandidates = useMemo<SupersetChartView[]>(() => {
    if (interactiveViewCandidates.length > 0) {
      return interactiveViewCandidates;
    }
    if (response?.embeddedId && response?.supersetDomain) {
      const fallbackLabel = resolveVizTypeLabel(
        response?.vizType,
        undefined,
        '看板'
      );
      return [
        {
          key: `dashboard-${response.embeddedId}`,
          label: fallbackLabel,
          vizType: response?.vizType || 'dashboard',
          vizName: fallbackLabel,
          embeddedId: response.embeddedId,
          supersetDomain: response.supersetDomain,
        },
      ];
    }
    return [];
  }, [
    response?.embeddedId,
    response?.supersetDomain,
    response?.vizType,
    interactiveViewCandidates,
  ]);

  const defaultViewKey = useMemo(() => {
    const matched = viewCandidates.find(candidate => candidate.vizType === response?.vizType);
    return matched?.key || viewCandidates[0]?.key || '';
  }, [response?.vizType, viewCandidates]);

  useEffect(() => {
    setActiveViewKey(defaultViewKey);
  }, [defaultViewKey]);

  const activeView = useMemo(() => {
    return viewCandidates.find(candidate => candidate.key === activeViewKey) || viewCandidates[0] || null;
  }, [activeViewKey, viewCandidates]);

  const activeMinHeight = useMemo(() => {
    const heightValue = activeView?.dashboardHeight ?? defaultMinHeight;
    const numericValue = typeof heightValue === 'number' ? heightValue : Number(heightValue);
    if (!Number.isFinite(numericValue) || numericValue <= 0) {
      return defaultMinHeight;
    }
    return numericValue;
  }, [activeView?.dashboardHeight, defaultMinHeight]);

  const hasFixedHeight = useMemo(() => {
    return Boolean(activeView?.dashboardHeight) || params?.some(
      (option: any) => option.paramType === 'FORWARD' && option.key === 'height'
    );
  }, [activeView?.dashboardHeight, params]);

  const embedInfo = useMemo(() => {
    if (activeView?.embeddedId && activeView?.supersetDomain) {
      return { embedId: activeView.embeddedId, supersetDomain: activeView.supersetDomain };
    }
    if (response?.embeddedId && response?.supersetDomain) {
      return { embedId: response.embeddedId, supersetDomain: response.supersetDomain };
    }
    return null;
  }, [activeView?.embeddedId, activeView?.supersetDomain, response?.embeddedId, response?.supersetDomain]);

  const canPushCurrentChart = Boolean(activeView?.chartId && response?.pluginId);
  const showSummary =
    interactiveViewCandidates.length > 0 || rawCandidateLabels.length > 0 || canPushCurrentChart;

  useEffect(() => {
    setHeight(activeMinHeight);
  }, [activeMinHeight]);

  useEffect(() => {
    if (!pushModalOpen) {
      setManualDashboards([]);
      setSelectedDashboardId(undefined);
      setNewDashboardTitle('');
      setDashboardLoading(false);
      setPushLoading(false);
    }
  }, [pushModalOpen]);

  const resolveBackgroundColor = useCallback(() => {
    return resolveHostBackgroundColor();
  }, []);

  const resolveThemeMode = useCallback((): ThemeMode => {
    return resolveHostThemeMode();
  }, []);

  const resolveThemeConfig = useCallback((): SupersetThemeConfig | undefined => {
    return resolveHostThemeConfig();
  }, []);

  const syncTheme = useCallback(
    async (instance?: EmbedInstance | null) => {
      const mode = resolveThemeMode();
      const background = resolveBackgroundColor();
      const themeConfig = resolveThemeConfig();
      backgroundColorRef.current = background;
      setBackgroundColor(background);
      const target = instance || embedInstanceRef.current;
      if (target?.setThemeMode) {
        try {
          await target.setThemeMode(mode);
        } catch (error) {
          // ignore theme sync error to avoid blocking rendering
        }
      }
      if (target?.setThemeConfig && themeConfig) {
        try {
          await target.setThemeConfig(themeConfig);
        } catch (error) {
          // ignore theme sync error to avoid blocking rendering
        }
      }
      const iframe = embedContainerRef.current?.querySelector('iframe');
      if (iframe && background) {
        iframe.style.backgroundColor = background;
      }
    },
    [resolveBackgroundColor, resolveThemeConfig, resolveThemeMode]
  );

  const computeAvailableHeight = useCallback(() => {
    if (typeof window === 'undefined') {
      return activeMinHeight;
    }
    if (hasFixedHeight) {
      return activeMinHeight;
    }
    const rect = embedContainerRef.current?.getBoundingClientRect();
    const top = rect ? rect.top : 0;
    const padding = 24;
    const scrollContainer = embedContainerRef.current?.closest?.(
      '#messageContainer'
    ) as HTMLElement | null;
    if (scrollContainer) {
      const containerRect = scrollContainer.getBoundingClientRect();
      const available = containerRect.bottom - top - padding;
      return Math.max(available, activeMinHeight);
    }
    const viewportHeight =
      window.innerHeight || document.documentElement.clientHeight || activeMinHeight;
    const available = viewportHeight - Math.max(top, 0) - padding;
    return Math.max(available, activeMinHeight);
  }, [activeMinHeight, hasFixedHeight]);

  const getDashboardScrollHeight = useCallback(async () => {
    const instance = embedInstanceRef.current;
    if (instance?.getScrollSize) {
      try {
        const size = await instance.getScrollSize();
        if (typeof size?.height === 'number' && Number.isFinite(size.height)) {
          return size.height;
        }
      } catch (error) {
        // ignore and fallback
      }
    }
    try {
      const iframe = embedContainerRef.current?.querySelector('iframe');
      const doc = iframe?.contentDocument || iframe?.contentWindow?.document;
      const bodyHeight = doc?.body?.scrollHeight;
      if (typeof bodyHeight === 'number' && Number.isFinite(bodyHeight) && bodyHeight > 0) {
        return bodyHeight;
      }
    } catch (error) {
      return null;
    }
    return null;
  }, []);

  const syncHeight = useCallback(async () => {
    if (hasFixedHeight) {
      setHeight(prev => (prev === activeMinHeight ? prev : activeMinHeight));
      return;
    }
    const baseHeight = computeAvailableHeight();
    const scrollHeight = await getDashboardScrollHeight();
    const nextHeight = scrollHeight && scrollHeight > baseHeight ? scrollHeight : baseHeight;
    setHeight(prev => (prev === nextHeight ? prev : nextHeight));
  }, [activeMinHeight, computeAvailableHeight, getDashboardScrollHeight, hasFixedHeight]);

  const handleOpenPushModal = useCallback(async () => {
    if (!canPushCurrentChart) {
      return;
    }
    setPushModalOpen(true);
    setDashboardLoading(true);
    try {
      const manageResp = unwrapApiEnvelope<SupersetDashboardManageResp>(
        await fetchSupersetManualDashboards(response?.pluginId)
      );
      const dashboards =
        manageResp && Array.isArray(manageResp.dashboards) ? manageResp.dashboards : [];
      setManualDashboards(dashboards);
      setSelectedDashboardId(dashboards[0]?.id);
    } catch (error: any) {
      message.error(error?.message || '获取看板列表失败');
    } finally {
      setDashboardLoading(false);
    }
  }, [canPushCurrentChart, response?.pluginId]);

  const handlePushExistingDashboard = useCallback(async () => {
    if (!canPushCurrentChart || !activeView?.chartId) {
      message.error('当前图表不可推送');
      return;
    }
    if (!selectedDashboardId) {
      message.error('请选择目标看板');
      return;
    }
    setPushLoading(true);
    try {
      const pushed = unwrapApiEnvelope<boolean>(
        await pushSupersetChartToDashboard({
          pluginId: response?.pluginId,
          dashboardId: selectedDashboardId,
          chartId: activeView.chartId,
        })
      );
      if (pushed !== true) {
        throw new Error('推送到看板失败');
      }
      message.success('已推送到看板');
      setPushModalOpen(false);
    } catch (error: any) {
      message.error(error?.message || '推送到看板失败');
    } finally {
      setPushLoading(false);
    }
  }, [activeView?.chartId, canPushCurrentChart, response?.pluginId, selectedDashboardId]);

  const handleCreateAndPushDashboard = useCallback(async () => {
    if (!canPushCurrentChart || !activeView?.chartId) {
      message.error('当前图表不可推送');
      return;
    }
    const title = newDashboardTitle.trim();
    if (!title) {
      message.error('请输入新看板名称');
      return;
    }
    setPushLoading(true);
    try {
      const dashboard = unwrapApiEnvelope<SupersetDashboardItem>(
        await createSupersetDashboard({
          pluginId: response?.pluginId,
          title,
        })
      );
      if (!dashboard?.id) {
        throw new Error('新建看板失败');
      }
      const pushed = unwrapApiEnvelope<boolean>(
        await pushSupersetChartToDashboard({
          pluginId: response?.pluginId,
          dashboardId: dashboard.id,
          chartId: activeView.chartId,
        })
      );
      if (pushed !== true) {
        throw new Error('推送到看板失败');
      }
      message.success('已新建并推送到看板');
      setPushModalOpen(false);
    } catch (error: any) {
      message.error(error?.message || '新建并推送失败');
    } finally {
      setPushLoading(false);
    }
  }, [activeView?.chartId, canPushCurrentChart, newDashboardTitle, response?.pluginId]);

  useEffect(() => {
    if (!embedInfo || !embedContainerRef.current) {
      return;
    }
    let cancelled = false;
    embedInstanceRef.current?.unmount();
    embedInstanceRef.current = null;
    embedContainerRef.current.replaceChildren();
    const fetchToken = async () => {
      const responseToken = await fetchSupersetGuestToken({
        pluginId: response?.pluginId,
        embeddedId: embedInfo.embedId,
      });
      const token = resolveGuestToken(responseToken);
      if (!token) {
        const payload = (responseToken as any)?.data ?? responseToken;
        const code = payload?.code;
        const msg = payload?.msg;
        const details = [code ? `code=${code}` : null, msg ? `msg=${msg}` : null]
          .filter(Boolean)
          .join(', ');
        throw new Error(details ? `guest token missing (${details})` : 'guest token missing');
      }
      return token;
    };
    embedDashboard({
      id: embedInfo.embedId,
      supersetDomain: embedInfo.supersetDomain,
      mountPoint: embedContainerRef.current,
      iframeTitle: SUPERSET_IFRAME_TITLE,
      fetchGuestToken: fetchToken,
      dashboardUiConfig: {
        hideTitle: true,
        hideTab: true,
        hideChartControls: false,
        filters: { visible: false, expanded: false },
      },
    })
      .then(instance => {
        if (cancelled) {
          instance.unmount();
          return;
        }
        embedInstanceRef.current = instance;
        syncTheme(instance);
        const iframe = embedContainerRef.current?.querySelector('iframe');
        if (iframe) {
          iframe.style.width = '100%';
          iframe.style.height = '100%';
          iframe.style.border = 'none';
          iframe.style.display = 'block';
          iframe.title = SUPERSET_IFRAME_TITLE;
          const currentBackground = backgroundColorRef.current || resolveBackgroundColor();
          if (currentBackground) {
            iframe.style.backgroundColor = currentBackground;
          }
        }
        const scheduleHeightSync = () => {
          let attempts = 0;
          const maxAttempts = 10;
          const interval = 250;
          const tick = () => {
            if (cancelled) {
              return;
            }
            attempts += 1;
            syncHeight();
            if (attempts < maxAttempts) {
              setTimeout(tick, interval);
            }
          };
          requestAnimationFrame(tick);
        };
        scheduleHeightSync();
      })
      .catch(() => {
        message.error('Superset 嵌入失败');
      });
    return () => {
      cancelled = true;
      embedInstanceRef.current?.unmount();
      embedInstanceRef.current = null;
    };
  }, [embedInfo, resolveBackgroundColor, response?.pluginId, syncHeight, syncTheme]);

  useEffect(() => {
    if (!embedInfo || typeof window === 'undefined') {
      return;
    }
    const handleResize = () => {
      syncHeight();
    };
    window.addEventListener('resize', handleResize);
    return () => {
      window.removeEventListener('resize', handleResize);
    };
  }, [embedInfo, syncHeight]);

  useEffect(() => {
    if (triggerResize) {
      syncHeight();
    }
  }, [syncHeight, triggerResize]);

  useEffect(() => {
    if (typeof window === 'undefined' || typeof MutationObserver === 'undefined') {
      return;
    }
    const handleThemeChange = () => {
      syncTheme();
    };
    const observer = new MutationObserver(handleThemeChange);
    observer.observe(document.documentElement, {
      attributes: true,
      attributeFilter: ['data-theme', 'class', 'style'],
    });
    if (document.body) {
      observer.observe(document.body, {
        attributes: true,
        attributeFilter: ['data-theme', 'class', 'style'],
      });
    }
    const media = window.matchMedia?.('(prefers-color-scheme: dark)');
    if (media) {
      const onMediaChange = () => handleThemeChange();
      if (typeof media.addEventListener === 'function') {
        media.addEventListener('change', onMediaChange);
      } else if (typeof media.addListener === 'function') {
        media.addListener(onMediaChange);
      }
      return () => {
        observer.disconnect();
        if (typeof media.removeEventListener === 'function') {
          media.removeEventListener('change', onMediaChange);
        } else if (typeof media.removeListener === 'function') {
          media.removeListener(onMediaChange);
        }
      };
    }
    return () => {
      observer.disconnect();
    };
  }, [syncTheme]);

  return (
    <>
      {showSummary && (
        <div
          data-testid="superset-chart-summary"
          style={{
            marginBottom: 12,
            padding: '10px 12px',
            borderRadius: 8,
            background: 'rgba(0, 0, 0, 0.04)',
          }}
        >
          <div
            style={{
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'space-between',
              flexWrap: 'wrap',
              gap: 8,
            }}
          >
            {interactiveViewCandidates.length > 0 && (
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8, flex: '1 1 auto' }}>
                {viewCandidates.map(view => (
                  <Button
                    key={view.key}
                    size="small"
                    type={activeView?.key === view.key ? 'primary' : 'default'}
                    onClick={() => setActiveViewKey(view.key)}
                  >
                    {view.label}
                  </Button>
                ))}
              </div>
            )}
            {interactiveViewCandidates.length === 0 && rawCandidateLabels.length > 0 && (
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8, flex: '1 1 auto' }}>
                {rawCandidateLabels.map(item => (
                  <span
                    key={item.key}
                    style={{
                      display: 'inline-flex',
                      alignItems: 'center',
                      padding: '2px 8px',
                      borderRadius: 999,
                      background: '#ffffff',
                      color: 'rgba(0, 0, 0, 0.75)',
                      fontSize: 12,
                      lineHeight: '20px',
                    }}
                  >
                    {item.label}
                  </span>
                ))}
              </div>
            )}
            {canPushCurrentChart && (
              <div style={{ display: 'flex', justifyContent: 'flex-end', marginLeft: 'auto' }}>
                <Button size="small" onClick={handleOpenPushModal}>
                  推送到看板
                </Button>
              </div>
            )}
          </div>
        </div>
      )}
      {embedInfo ? (
        <div
          ref={embedContainerRef}
          data-embed-id={id}
          style={{
            width: '100%',
            height,
            backgroundColor,
          }}
        />
      ) : (
        <div data-embed-id={id} style={{ width: '100%', height, backgroundColor }}>
          Superset 嵌入信息缺失，无法渲染看板。
        </div>
      )}
      <Modal
        title="推送到看板"
        open={pushModalOpen}
        onCancel={() => setPushModalOpen(false)}
        footer={null}
        destroyOnClose
      >
        <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
          <div style={{ fontSize: 13, fontWeight: 600 }}>选择现有看板</div>
          {dashboardLoading ? (
            <div>加载中...</div>
          ) : manualDashboards.length > 0 ? (
            <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
              {manualDashboards.map(dashboard => (
                <Button
                  key={dashboard.id}
                  type={selectedDashboardId === dashboard.id ? 'primary' : 'default'}
                  onClick={() => setSelectedDashboardId(dashboard.id)}
                >
                  {dashboard.title || `看板 ${dashboard.id}`}
                </Button>
              ))}
            </div>
          ) : (
            <div>暂无手工看板，可直接新建并推送。</div>
          )}
          <div style={{ display: 'flex', justifyContent: 'flex-end' }}>
            <Button
              type="primary"
              disabled={!selectedDashboardId}
              loading={pushLoading}
              onClick={handlePushExistingDashboard}
            >
              推送到所选看板
            </Button>
          </div>
          <div
            style={{
              borderTop: '1px solid rgba(0, 0, 0, 0.08)',
              paddingTop: 12,
              display: 'flex',
              flexDirection: 'column',
              gap: 12,
            }}
          >
            <div style={{ fontSize: 13, fontWeight: 600 }}>新建看板</div>
            <Input
              placeholder="输入新看板名称"
              value={newDashboardTitle}
              onChange={event => setNewDashboardTitle(event.target.value)}
              maxLength={120}
            />
            <div style={{ display: 'flex', justifyContent: 'flex-end' }}>
              <Button type="primary" loading={pushLoading} onClick={handleCreateAndPushDashboard}>
                新建并推送
              </Button>
            </div>
          </div>
        </div>
      </Modal>
    </>
  );
};

export default SupersetChart;
