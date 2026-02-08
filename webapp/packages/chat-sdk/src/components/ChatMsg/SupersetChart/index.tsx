import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Button, Dropdown, Segmented, message } from 'antd';
import { embedDashboard } from '@superset-ui/embedded-sdk';
import type { EmbeddedDashboard, ThemeMode } from '@superset-ui/embedded-sdk';
import {
  MsgDataType,
  SupersetChartResponseType,
  SupersetDashboardType,
  SupersetVizTypeCandidate,
} from '../../../common/type';
import {
  fetchSupersetDashboards,
  fetchSupersetGuestToken,
  pushSupersetChartToDashboard,
} from '../../../service';

type Props = {
  id: string | number;
  data: MsgDataType;
  triggerResize?: boolean;
};

const DEFAULT_HEIGHT = 800;
const SUPERSET_SINGLE_CHART_PREFIX = 'supersonic_';
const SUPERSET_IFRAME_TITLE = 'supersetIframe';

type EmbedInstance = EmbeddedDashboard;

export const filterTemporaryDashboards = (
  dashboardList: SupersetDashboardType[],
  pluginName?: string
) => {
  if (!Array.isArray(dashboardList) || dashboardList.length === 0) {
    return [];
  }
  if (!pluginName) {
    return dashboardList;
  }
  const prefix = `${SUPERSET_SINGLE_CHART_PREFIX}${pluginName}_`;
  return dashboardList.filter(dashboard => {
    const title = dashboard?.title || '';
    return !title.startsWith(prefix);
  });
};

const SupersetChart: React.FC<Props> = ({ id, data, triggerResize }) => {
  const [height, setHeight] = useState(DEFAULT_HEIGHT);
  const [backgroundColor, setBackgroundColor] = useState<string>();
  const [dashboards, setDashboards] = useState<SupersetDashboardType[]>([]);
  const [dashboardsLoading, setDashboardsLoading] = useState(false);
  const [pushLoading, setPushLoading] = useState(false);
  const [candidateIndex, setCandidateIndex] = useState(0);
  const embedContainerRef = useRef<HTMLDivElement>(null);
  const embedInstanceRef = useRef<EmbedInstance | null>(null);
  const backgroundColorRef = useRef<string>();
  const response = data.response as SupersetChartResponseType;
  const webPage = response?.webPage;
  const vizTypeCandidates = useMemo(() => {
    const candidates = response?.vizTypeCandidates;
    if (Array.isArray(candidates) && candidates.length > 0) {
      return candidates.filter(Boolean);
    }
    if (response?.embeddedId || response?.supersetDomain || response?.chartId) {
      return [
        {
          vizType: response?.vizType || '',
          vizName: response?.vizType,
          chartId: response?.chartId,
          chartUuid: response?.chartUuid,
          guestToken: response?.guestToken,
          embeddedId: response?.embeddedId,
          supersetDomain: response?.supersetDomain,
        },
      ];
    }
    return [];
  }, [response]);
  const activeCandidate = useMemo(() => {
    if (!vizTypeCandidates.length) {
      return null;
    }
    const index = Math.min(candidateIndex, vizTypeCandidates.length - 1);
    return vizTypeCandidates[index];
  }, [candidateIndex, vizTypeCandidates]);
  const resolveGuestToken = (payload: any) => {
    if (!payload) {
      return '';
    }
    if (typeof payload === 'string') {
      return payload;
    }
    if (typeof payload.token === 'string' && payload.token) {
      return payload.token;
    }
    if (typeof payload?.data?.token === 'string' && payload.data.token) {
      return payload.data.token;
    }
    return '';
  };

  const params = useMemo(() => {
    const rawParams = webPage?.params || webPage?.paramOptions || [];
    return Array.isArray(rawParams) ? rawParams : [];
  }, [webPage]);

  const minHeight = useMemo(() => {
    const heightValue =
      params?.find((option: any) => option.paramType === 'FORWARD' && option.key === 'height')
        ?.value ?? DEFAULT_HEIGHT;
    const numericValue = typeof heightValue === 'number' ? heightValue : Number(heightValue);
    if (!Number.isFinite(numericValue) || numericValue <= 0) {
      return DEFAULT_HEIGHT;
    }
    return numericValue;
  }, [params]);

  useEffect(() => {
    setCandidateIndex(0);
  }, [id]);

  useEffect(() => {
    setCandidateIndex(prev => (prev < vizTypeCandidates.length ? prev : 0));
  }, [vizTypeCandidates.length]);

  const embedInfo = useMemo(() => {
    const embeddedId = activeCandidate ? activeCandidate.embeddedId : response?.embeddedId;
    const supersetDomain = activeCandidate
      ? activeCandidate.supersetDomain
      : response?.supersetDomain;
    if (embeddedId && supersetDomain) {
      return { embedId: embeddedId, supersetDomain };
    }
    return null;
  }, [
    activeCandidate?.embeddedId,
    activeCandidate?.supersetDomain,
    response?.embeddedId,
    response?.supersetDomain,
  ]);

  useEffect(() => {
    setHeight(minHeight);
  }, [minHeight]);

  const resolveThemeMode = useCallback((): ThemeMode => {
    if (typeof document === 'undefined') {
      return 'light' as ThemeMode;
    }
    const docElement = document.documentElement;
    const themeAttr = docElement.getAttribute('data-theme') || docElement.dataset.theme || '';
    const normalized = themeAttr.toLowerCase();
    if (normalized.includes('dark')) {
      return 'dark' as ThemeMode;
    }
    if (normalized.includes('light')) {
      return 'light' as ThemeMode;
    }
    const bodyClass = document.body?.className || '';
    if (bodyClass.includes('dark')) {
      return 'dark' as ThemeMode;
    }
    if (bodyClass.includes('light')) {
      return 'light' as ThemeMode;
    }
    if (typeof window !== 'undefined' && window.matchMedia) {
      return (window.matchMedia('(prefers-color-scheme: dark)').matches
        ? 'dark'
        : 'light') as ThemeMode;
    }
    return 'light' as ThemeMode;
  }, []);

  const resolveBackgroundColor = useCallback(() => {
    if (typeof window === 'undefined') {
      return undefined;
    }
    const rootStyle = window.getComputedStyle(document.documentElement);
    const bodyStyle = window.getComputedStyle(document.body);
    const candidates = [
      rootStyle.getPropertyValue('--component-background'),
      rootStyle.getPropertyValue('--body-background'),
      rootStyle.getPropertyValue('--light-background'),
      bodyStyle.backgroundColor,
      '#ffffff',
    ];
    const resolved = candidates.find(value => value && value.trim())?.trim();
    return resolved || '#ffffff';
  }, []);

  const syncTheme = useCallback(
    async (instance?: EmbedInstance | null) => {
      const mode = resolveThemeMode();
      const background = resolveBackgroundColor();
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
      const iframe = embedContainerRef.current?.querySelector('iframe');
      if (iframe && background) {
        iframe.style.backgroundColor = background;
      }
    },
    [resolveBackgroundColor, resolveThemeMode]
  );

  const computeAvailableHeight = useCallback(() => {
    if (typeof window === 'undefined') {
      return minHeight;
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
      return Math.max(available, minHeight);
    }
    const viewportHeight = window.innerHeight || document.documentElement.clientHeight || minHeight;
    const available = viewportHeight - Math.max(top, 0) - padding;
    return Math.max(available, minHeight);
  }, [minHeight]);

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
    const baseHeight = computeAvailableHeight();
    const scrollHeight = await getDashboardScrollHeight();
    const nextHeight = scrollHeight && scrollHeight > baseHeight ? scrollHeight : baseHeight;
    setHeight(prev => (prev === nextHeight ? prev : nextHeight));
  }, [computeAvailableHeight, getDashboardScrollHeight]);

  useEffect(() => {
    if (!embedInfo || !embedContainerRef.current) {
      return;
    }
    let cancelled = false;
    embedInstanceRef.current?.unmount();
    embedInstanceRef.current = null;
    embedContainerRef.current.replaceChildren();
    const fetchGuestToken = async () => {
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
      fetchGuestToken,
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
    if (Array.isArray(response?.dashboards)) {
      setDashboards(response?.dashboards || []);
    }
  }, [response?.dashboards]);

  useEffect(() => {
    const shouldFetch = response?.dashboards === undefined && response?.pluginId;
    if (!shouldFetch) {
      return;
    }
    setDashboardsLoading(true);
    fetchSupersetDashboards(response.pluginId)
      .then(res => {
        setDashboards(res?.data || []);
      })
      .catch(() => {
        message.error('获取 Dashboard 列表失败');
      })
      .finally(() => {
        setDashboardsLoading(false);
      });
  }, [response?.dashboards, response?.pluginId]);

  useEffect(() => {
    if (!embedInfo) {
      return;
    }
    if (typeof window === 'undefined') {
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

  const activeChartId = activeCandidate ? activeCandidate.chartId : response?.chartId;

  const handlePush = (dashboardId?: number) => {
    if (!dashboardId) {
      return;
    }
    if (!activeChartId) {
      message.error('Chart 信息缺失');
      return;
    }
    setPushLoading(true);
    pushSupersetChartToDashboard({
      pluginId: response?.pluginId,
      dashboardId,
      chartId: activeChartId,
    })
      .then(() => {
        message.success('已推送到看板');
      })
      .catch(() => {
        message.error('推送失败');
      })
      .finally(() => {
        setPushLoading(false);
      });
  };

  const filteredDashboards = useMemo(
    () => filterTemporaryDashboards(dashboards, response?.name),
    [dashboards, response?.name]
  );

  const menuItems =
    filteredDashboards.length > 0
      ? filteredDashboards.map(item => ({
          key: String(item.id ?? item.title ?? 'unknown'),
          label: item.title || `Dashboard ${item.id}`,
        }))
      : [
          {
            key: 'empty',
            label: '暂无 Dashboard',
            disabled: true,
          },
        ];

  const candidateOptions = useMemo(
    () =>
      vizTypeCandidates.map((candidate: SupersetVizTypeCandidate, index: number) => ({
        label: candidate?.vizName || candidate?.vizType || `候选${index + 1}`,
        value: index,
      })),
    [vizTypeCandidates]
  );
  const showCandidateSwitch = candidateOptions.length > 1;
  const showPushButton = Boolean(!response?.fallback && response?.pluginId && activeChartId);

  return (
    <>
      {showPushButton && (
        <div style={{ display: 'flex', justifyContent: 'flex-end', marginBottom: 8 }}>
          <Dropdown
            menu={{
              items: menuItems,
              onClick: info => {
                const dashboardId = Number(info.key);
                handlePush(Number.isNaN(dashboardId) ? undefined : dashboardId);
              },
            }}
            disabled={dashboardsLoading}
          >
            <Button size="small" loading={dashboardsLoading || pushLoading}>
              推送到看板
            </Button>
          </Dropdown>
        </div>
      )}
      {showCandidateSwitch && (
        <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 8 }}>
          <span style={{ color: 'rgba(0, 0, 0, 0.45)', fontSize: 12 }}>图表形式</span>
          <Segmented
            size="small"
            options={candidateOptions}
            value={candidateIndex}
            onChange={value => {
              const resolved = typeof value === 'number' ? value : Number(value);
              setCandidateIndex(Number.isFinite(resolved) ? resolved : 0);
            }}
          />
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
    </>
  );
};

export default SupersetChart;
