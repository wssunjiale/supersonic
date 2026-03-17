import { useLayoutEffect } from 'react';
import { useLocation } from '@umijs/max';
import { getToken } from '@/utils/utils';
import queryString from 'query-string';
import { Chat } from 'supersonic-chat-sdk';
import {
  applyChatPageLayoutClass,
  CHAT_PAGE_ROOT_CLASS_NAME,
} from './layout';

const ChatPage = () => {
  const location = useLocation();
  const query = queryString.parse(location.search) || {};
  const { agentId } = query;

  useLayoutEffect(() => {
    return applyChatPageLayoutClass(document);
  }, []);

  return (
    <div className={CHAT_PAGE_ROOT_CLASS_NAME}>
      <Chat initialAgentId={agentId ? +agentId : undefined} token={getToken() || ''} isDeveloper />
    </div>
  );
};

export default ChatPage;
