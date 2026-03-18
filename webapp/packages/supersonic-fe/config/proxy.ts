export default {
  dev: {
    '/api/': {
      target: 'http://127.0.0.1:9090',
      changeOrigin: true,
    },
    '/aibi/api/': {
      target: 'http://127.0.0.1:9090',
      changeOrigin: true,
    },
  },
};
