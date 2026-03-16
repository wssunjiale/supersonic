import { render, screen, waitFor } from '@testing-library/react';
import ChatItem from './index';

jest.mock('./ExecuteItem', () => (props: any) => (
  <div
    data-testid="execute-item"
    data-has-data={String(Boolean(props.data))}
    data-query-mode={props.data?.queryMode || ''}
    data-execute-tip={props.executeTip || ''}
  />
));
jest.mock('./ExpandParseTip', () => () => null);
jest.mock('./ParseTip', () => () => null);
jest.mock('./SqlItem', () => () => null);
jest.mock('./SimilarQuestionItem', () => () => null);
jest.mock('../Tools', () => () => null);
jest.mock('../IconFont', () => () => null);
jest.mock('../../service', () => ({
  chatExecute: jest.fn(),
  chatParse: jest.fn(),
  queryData: jest.fn(),
  deleteQuery: jest.fn(),
  switchEntity: jest.fn(),
  getExecuteSummary: jest.fn(),
}));
jest.mock('../../utils/utils', () => ({
  isMobile: false,
  exportCsvFile: jest.fn(),
}));
jest.mock('../../hooks', () => ({
  useMethodRegister: () => ({
    register: jest.fn(),
    call: jest.fn(),
  }),
}));
jest.mock('antd', () => {
  const actual = jest.requireActual('antd');
  return {
    ...actual,
    Spin: ({ children }: any) => <>{children}</>,
    message: {
      success: jest.fn(),
      error: jest.fn(),
    },
  };
});

describe('ChatItem', () => {
  test('accepts successful superset results even when queryColumns are empty', async () => {
    render(
      <ChatItem
        msg=""
        conversationId={20}
        msgData={
          {
            queryId: 66,
            queryMode: 'SUPERSET',
            queryState: 'SUCCESS',
            queryColumns: [],
            queryResults: [],
            response: {
              name: 'Superset',
              pluginId: 2,
              pluginType: 'SUPERSET',
              fallback: false,
              vizType: 'table',
              webPage: {
                url: '',
                params: [],
                paramOptions: [],
                valueParams: [],
              },
            },
            chatContext: {
              id: 1,
              dimensionFilters: [],
              dateInfo: {},
            },
            similarQueries: [],
          } as any
        }
      />
    );

    await waitFor(() => {
      const executeItem = screen.getByTestId('execute-item');
      expect(executeItem).toHaveAttribute('data-has-data', 'true');
      expect(executeItem).toHaveAttribute('data-query-mode', 'SUPERSET');
      expect(executeItem).toHaveAttribute('data-execute-tip', '');
    });
  });
});
