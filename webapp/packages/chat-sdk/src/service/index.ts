import axios from './axiosInstance';
import {
  ChatContextType,
  HistoryMsgItemType,
  HistoryType,
  MsgDataType,
  ParseDataType,
  SearchRecommendItem,
  SupersetDashboardItem,
  SupersetDashboardManageResp,
  SupersetGuestTokenResp,
} from '../common/type';
import { isMobile } from '../utils/utils';

const DEFAULT_CHAT_ID = 0;

const prefix = isMobile ? '/openapi' : '/api';

export function searchRecommend(
  queryText: string,
  chatId?: number,
  modelId?: number,
  agentId?: number
) {
  return axios.post<SearchRecommendItem[]>(`${prefix}/chat/query/search`, {
    queryText,
    chatId: chatId || DEFAULT_CHAT_ID,
    modelId,
    agentId,
  });
}

export function chatQuery(queryText: string, chatId?: number, modelId?: number, filters?: any[]) {
  return axios.post<MsgDataType>(`${prefix}/chat/query/query`, {
    queryText,
    chatId: chatId || DEFAULT_CHAT_ID,
    modelId,
    queryFilters: filters
      ? {
          filters,
        }
      : undefined,
  });
}

export function chatParse({
  queryText,
  chatId,
  modelId,
  agentId,
  parseId,
  queryId,
  filters,
  parseInfo,
}: {
  queryText: string;
  chatId?: number;
  modelId?: number;
  agentId?: number;
  queryId?: number;
  parseId?: number;
  filters?: any[];
  parseInfo?: ChatContextType;
}) {
  return axios.post<ParseDataType>(`${prefix}/chat/query/parse`, {
    queryText,
    chatId: chatId || DEFAULT_CHAT_ID,
    dataSetId: modelId,
    agentId,
    parseId,
    queryId,
    selectedParse: parseInfo,
    queryFilters: filters
      ? {
          filters,
        }
      : undefined,
  });
}

export function chatExecute(
  queryText: string,
  chatId: number,
  parseInfo: ChatContextType,
  agentId?: number,
  streamingResult?:boolean
) {
  // AgentService executes external agent calls that may take a long time.
  // Override axiosInstance default timeout (120s) to avoid frontend aborting the request.
  const requestConfig =
    parseInfo?.queryMode === 'AGENT_SERVICE' ? { timeout: 0 } : undefined;
  return axios.post<MsgDataType>(
    `${prefix}/chat/query/execute`,
    {
      queryText,
      agentId,
      chatId: chatId || DEFAULT_CHAT_ID,
      queryId: parseInfo.queryId,
      parseId: parseInfo.id,
      streamingResult:streamingResult
    },
    requestConfig as any
  );
}

export function getExecuteSummary(
    queryId: number
) {
  return axios.post<MsgDataType>(`${prefix}/chat/query/getExecuteSummary`, {
    queryId: queryId,
  });
}

export function fetchSupersetGuestToken(
  params: { pluginId?: number; embeddedId: string }
): Promise<SupersetGuestTokenResp> {
  return axios.post(`${prefix}/chat/superset/guest-token`, params) as unknown as Promise<SupersetGuestTokenResp>;
}

export function fetchSupersetManualDashboards(
  pluginId?: number
): Promise<SupersetDashboardManageResp> {
  return axios.post(`${prefix}/chat/superset/dashboards/manage`, {
    pluginId,
  }) as unknown as Promise<SupersetDashboardManageResp>;
}

export function createSupersetDashboard(params: {
  pluginId?: number;
  title: string;
}): Promise<SupersetDashboardItem> {
  return axios.post(`${prefix}/chat/superset/dashboard/create`, params) as unknown as Promise<SupersetDashboardItem>;
}

export function pushSupersetChartToDashboard(params: {
  pluginId?: number;
  dashboardId: number;
  chartId: number;
}): Promise<boolean> {
  return axios.post(`${prefix}/chat/superset/dashboard/push`, params) as unknown as Promise<boolean>;
}

export function switchEntity(entityId: string, modelId?: number, chatId?: number) {
  return axios.post<any>(`${prefix}/chat/query/switchQuery`, {
    queryText: entityId,
    modelId,
    chatId: chatId || DEFAULT_CHAT_ID,
  });
}

export function queryData(chatContext: Partial<ChatContextType>) {
  return axios.post<MsgDataType>(`${prefix}/chat/query/queryData`, chatContext);
}

export function getHistoryMsg(
  current: number,
  chatId: number = DEFAULT_CHAT_ID,
  pageSize: number = 10
) {
  return axios.post<HistoryType>(`${prefix}/chat/manage/pageQueryInfo?chatId=${chatId}`, {
    current,
    pageSize,
  });
}

export function querySimilarQuestions(queryId: number) {
  return axios.get<HistoryMsgItemType>(`${prefix}/chat/manage/getChatQuery/${queryId}`);
}

export function deleteQuery(queryId: number) {
  return axios.delete<any>(`${prefix}/chat/manage/${queryId}`);
}

export function queryEntities(entityId: string | number, modelId: number) {
  return axios.post<any>(`${prefix}/chat/query/choice`, {
    entityId,
    modelId,
  });
}

export function updateQAFeedback(questionId: number, score: number) {
  return axios.post<any>(
    `${prefix}/chat/manage/updateQAFeedback?id=${questionId}&score=${score}&feedback=`
  );
}

export function queryDimensionValues(
  modelId: number,
  bizName: string,
  agentId: number,
  elementID: number,
  value: string
) {
  return axios.post<any>(`${prefix}/chat/query/queryDimensionValue`, {
    modelId,
    bizName,
    agentId,
    elementID,
    value,
  });
}
