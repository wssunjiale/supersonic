import assert from 'node:assert/strict';
import {
  applyChatPageLayoutClass,
  CHAT_PAGE_CLASS_NAME,
  CHAT_PAGE_ROOT_CLASS_NAME,
  getChatChildrenWrapperStyle,
  getChatContentStyle,
} from './layout';

const createClassList = () => {
  const tokens = new Set<string>();

  return {
    add: (...items: string[]) => items.forEach((item) => tokens.add(item)),
    remove: (...items: string[]) => items.forEach((item) => tokens.delete(item)),
    has: (item: string) => tokens.has(item),
  };
};

const documentElementClassList = createClassList();
const bodyClassList = createClassList();

const cleanup = applyChatPageLayoutClass({
  documentElement: { classList: documentElementClassList },
  body: { classList: bodyClassList },
});

assert.equal(documentElementClassList.has(CHAT_PAGE_CLASS_NAME), true);
assert.equal(bodyClassList.has(CHAT_PAGE_CLASS_NAME), true);

cleanup();

assert.equal(documentElementClassList.has(CHAT_PAGE_CLASS_NAME), false);
assert.equal(bodyClassList.has(CHAT_PAGE_CLASS_NAME), false);

const contentStyle = getChatContentStyle({ background: 'red' });
assert.equal(contentStyle.background, 'red');
assert.equal(contentStyle.minHeight, 0);
assert.equal(contentStyle.height, 'calc(100dvh - 56px)');
assert.equal(contentStyle.overflow, 'hidden');

const wrapperStyle = getChatChildrenWrapperStyle();
assert.equal(wrapperStyle.display, 'flex');
assert.equal(wrapperStyle.flexDirection, 'column');
assert.equal(wrapperStyle.flex, 1);
assert.equal(wrapperStyle.minHeight, 0);
assert.equal(wrapperStyle.height, '100%');
assert.equal(wrapperStyle.overflow, 'hidden');

assert.equal(CHAT_PAGE_ROOT_CLASS_NAME, 'ss-chat-page-root');

console.log('chat-page-layout-test: ok');
