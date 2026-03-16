import { render } from '@testing-library/react';
import ChatMsg from './index';

jest.mock('./Table', () => () => <div title="table-view" />);
jest.mock('./Pie', () => () => <div title="pie-view" />);
jest.mock('./Bar', () => () => <div title="bar-view" />);
jest.mock('./MetricCard', () => () => <div title="metric-card-view" />);
jest.mock('./MetricTrend', () => () => <div title="trend-view" />);
jest.mock('./MarkDown', () => () => <div title="markdown-view" />);
jest.mock('./Text', () => () => <div title="text-view" />);
jest.mock('../MetricOptions', () => () => null);
jest.mock('../DrillDownDimensions', () => () => null);
jest.mock('../../service', () => ({
  queryData: jest.fn(),
}));
jest.mock('../../utils/utils', () => ({
  isMobile: false,
}));

describe('ChatMsg', () => {
  test('does not force detail queries with chartable results into table mode', () => {
    const data = {
      queryMode: 'SUPERSET',
      queryColumns: [
        {
          name: '品牌名称',
          nameEn: 'brand_name',
          bizName: 'brand_name',
          showType: 'CATEGORY',
          type: 'STRING',
        },
        {
          name: '收入',
          nameEn: 'income',
          bizName: 'income',
          showType: 'NUMBER',
          type: 'NUMBER',
        },
      ],
      queryResults: [
        { brand_name: 'Office', income: 30 },
        { brand_name: 'Windows', income: 70 },
      ],
      chatContext: {
        queryType: 'DETAIL',
        queryMode: 'DETAIL_DIMENSION',
        metrics: [{ bizName: 'income', name: '收入' }],
        dimensions: [{ bizName: 'brand_name', name: '品牌名称' }],
      },
    } as any;

    const { getByTitle, queryByTitle } = render(
      <ChatMsg
        queryId={1}
        question="各品牌收入比例"
        data={data}
        chartIndex={0}
        onMsgContentTypeChange={() => {}}
      />
    );

    expect(getByTitle('pie-view')).toBeInTheDocument();
    expect(queryByTitle('table-view')).toBeNull();
  });
});
