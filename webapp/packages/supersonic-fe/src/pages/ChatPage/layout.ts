import type { CSSProperties } from 'react';

export const CHAT_PAGE_CLASS_NAME = 'ss-chat-page';
export const CHAT_PAGE_ROOT_CLASS_NAME = 'ss-chat-page-root';

type ClassListTarget = {
  classList: {
    add: (...tokens: string[]) => void;
    remove: (...tokens: string[]) => void;
  };
};

type ChatPageDocumentLike = {
  documentElement?: ClassListTarget | null;
  body?: ClassListTarget | null;
};

export const getChatContentStyle = (
  baseStyle: CSSProperties = {},
): CSSProperties => ({
  ...baseStyle,
  minHeight: 0,
  height: 'calc(100dvh - 56px)',
  overflow: 'hidden',
});

export const getChatChildrenWrapperStyle = (): CSSProperties => ({
  display: 'flex',
  flexDirection: 'column',
  flex: 1,
  minWidth: 0,
  minHeight: 0,
  height: '100%',
  overflow: 'hidden',
});

export const applyChatPageLayoutClass = (doc: ChatPageDocumentLike) => {
  doc.documentElement?.classList.add(CHAT_PAGE_CLASS_NAME);
  doc.body?.classList.add(CHAT_PAGE_CLASS_NAME);

  return () => {
    doc.documentElement?.classList.remove(CHAT_PAGE_CLASS_NAME);
    doc.body?.classList.remove(CHAT_PAGE_CLASS_NAME);
  };
};
