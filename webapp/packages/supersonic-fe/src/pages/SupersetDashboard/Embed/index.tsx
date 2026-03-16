import { useLocation } from '@umijs/max';
import { embedDashboard } from '@superset-ui/embedded-sdk';
import { message } from 'antd';
import queryString from 'query-string';
import React, { useEffect, useMemo, useRef } from 'react';
import { fetchSupersetGuestToken } from '../service';

type ApiEnvelope<T> = {
  code?: number | string;
  msg?: string;
  data?: T;
};

const normalizeDomain = (domain?: string) => {
  if (!domain) {
    return undefined;
  }
  return domain.endsWith('/') ? domain.slice(0, -1) : domain;
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

const SupersetDashboardEmbed: React.FC = () => {
  const location = useLocation();
  const query = useMemo(() => queryString.parse(location.search) || {}, [location.search]);
  const embeddedId = typeof query.embeddedId === 'string' ? query.embeddedId : '';
  const supersetDomain = normalizeDomain(
    typeof query.supersetDomain === 'string' ? query.supersetDomain : undefined
  );
  const pluginId = query.pluginId ? Number(query.pluginId) : undefined;
  const title = typeof query.title === 'string' ? query.title : 'Superset Dashboard';
  const containerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!embeddedId || !supersetDomain || !containerRef.current) {
      return;
    }
    let cancelled = false;
    const container = containerRef.current;
    const syncIframeStyle = () => {
      const iframe = container.querySelector('iframe');
      if (iframe instanceof HTMLIFrameElement) {
        iframe.style.display = 'block';
        iframe.style.width = '100%';
        iframe.style.height = '100%';
        iframe.style.border = '0';
      }
    };
    const observer = new MutationObserver(() => {
      syncIframeStyle();
    });
    observer.observe(container, { childList: true, subtree: true });
    const fetchGuestToken = async () => {
      const resp = unwrapApiEnvelope<{ token?: string }>(
        await fetchSupersetGuestToken({ pluginId, embeddedId })
      );
      const token = resp?.token;
      if (!token) {
        throw new Error('guest token missing');
      }
      return token;
    };
    embedDashboard({
      id: embeddedId,
      supersetDomain,
      mountPoint: container,
      iframeTitle: title,
      fetchGuestToken,
      dashboardUiConfig: {
        hideTitle: false,
        hideTab: true,
        hideChartControls: false,
        filters: { visible: false, expanded: false },
      },
    })
      .then(() => {
        syncIframeStyle();
      })
      .catch(() => {
        if (!cancelled) {
          message.error('Superset 嵌入失败');
        }
      });
    return () => {
      cancelled = true;
      observer.disconnect();
      container.replaceChildren();
    };
  }, [embeddedId, supersetDomain, pluginId, title]);

  if (!embeddedId || !supersetDomain) {
    return <div style={{ padding: 24 }}>嵌入信息缺失，无法渲染看板。</div>;
  }

  return (
    <div style={{ width: '100vw', height: '100vh', background: '#fff' }}>
      <div ref={containerRef} style={{ width: '100%', height: '100%' }} />
    </div>
  );
};

export default SupersetDashboardEmbed;
