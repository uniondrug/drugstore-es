<?php
/**
 * Created by PhpStorm.
 * User: lvchaohui
 * Date: 2021/5/7
 * Time: 1:51 PM
 */
namespace Uniondrug\DrugstoreEs;

use App\Errors\Error;
use Elasticsearch\Client;
use Uniondrug\Framework\Services\Service;

/**
 * Class EsService
 * @package Uniondrug\DrugstoreEs
 * @property Client $esClient
 */
class EsService extends Service
{
    /**
     * 分页搜索
     * {"page":1,"limit":10,"term":[{"store_organ_id":"4123"}],"terms":[],"sort":[{"name":"location","type":"location","lat":"32","lon":"120"},{"name":"star_class","type":"int","order":"desc"}]}
     * @param $index
     * @param $type
     * @param $data
     */
    public function paging($index, $type, $data)
    {
        // 一页数量
        $size = !empty($data['limit']) ? $data['limit'] : 1;
        // 页码
        $from = !empty($data['page']) ? ($data['page'] - 1) * $data['limit'] : 0;
        // 必须查询
        $must = [];
        if (isset($data['must']) && $data['must']) {
            if (isset($data['must']['term']) && $data['must']['term']) {
                foreach ($data['must']['term'] as $key => $value) {
                    $must[] = [
                        'term' => $value
                    ];
                }
            }
            if (isset($data['must']['terms']) && $data['must']['terms']) {
                foreach ($data['must']['terms'] as $key => $value) {
                    $must[] = [
                        'terms' => $value
                    ];
                }
            }
        }
        $should = [];
        if (isset($data['should']) && $data['should']) {
            if (isset($data['should']['term']) && $data['should']['term']) {
                foreach ($data['should']['term'] as $key => $value) {
                    $should[] = [
                        'term' => $value
                    ];
                }
            }
            if (isset($data['should']['terms']) && $data['should']['terms']) {
                foreach ($data['should']['terms'] as $key => $value) {
                    $should[] = [
                        'terms' => $value
                    ];
                }
            }
        }
        $query = [];
        if ($should) {
            $query['should'] = $should;
            if ($must) {
                $query['should'][] = [
                    'bool' => [
                        'must' => $must
                    ]
                ];
            }
        } else {
            $query['must'] = $must;
        }
        // 排序
        $sort = [];
        if (isset($data['sort'])) {
            foreach ($data['sort'] as $key => $value) {
                if ($value['type'] == 'location') {
                    if ($value['lat'] && $value['lon']) {
                        $sort[] = [
                            '_geo_distance' => [
                                $value['name'] => [
                                    'lat' => $value['lat'],
                                    'lon' => $value['lon']
                                ],
                                "order" => isset($value['order']) ? $value['order'] : 'asc',
                                "unit" => isset($value['unit']) ? $value['unit'] : 'm'
                            ]
                        ];
                    }
                }
                if ($value['type'] == 'int') {
                    $sort[] = [
                        $value['name'] => [
                            "order" => isset($value['order']) ? $value['order'] : 'asc'
                        ]
                    ];
                }
            }
        }
        $params = [
            'index' => $index,
            'type' => $type,
            'body' => [
                'from' => $from,
                'size' => $size,
                'sort' => $sort,
                'query' => [
                    'bool' => $query
                ]
            ]
        ];
        // 获取总数
        $count = $this->count($index, $type, $query);
        $result = $this->esClient->search($params);
        $items = [];
        if (isset($result['hits']) && $result['hits'] && isset($result['hits']['hits']) && $result['hits']['hits']) {
            foreach ($result['hits']['hits'] as $hit) {
                $distance = -1;
                if ($sort) {
                    foreach ($sort as $key => $item) {
                        foreach ($item as $k => $v) {
                            if ($k == '_geo_distance') {
                                $distance = $hit['sort'][$key];
                            }
                        }
                    }
                }
                $hit['_source']['distance'] = (int) $distance;
                $items[] = $hit['_source'];
            }
        }
        $totalPage = (int) ($count / $size) + 1;
        $paging = [
            "first" => 1,
            "before" => $data['page'] > 1 ? $data['page'] - 1 : 1,
            "current" => $data['page'],
            "last" => $totalPage,
            "next" => $totalPage > $data['page'] ? $data['page'] + 1 : $totalPage,
            "limit" => $size,
            "totalPages" => $totalPage,
            "totalItems" => $count
        ];
        return [
            'paging' => $paging,
            'body' => $items
        ];
    }

    /**
     * 获取数量
     * @param $index
     * @param $type
     * @param $data
     * @return int
     */
    public function count($index, $type, $query)
    {
        $params = [
            'index' => $index,
            'type' => $type,
            'body' => [
                'query' => [
                    'bool' => $query
                ]
            ]
        ];
        $count = $this->esClient->count($params);
        return $count['count'];
    }

    /**
     * 创建数据
     * @param $index
     * @param $type
     * @param $data
     * @return array
     */
    public function create($index, $type, $data)
    {
        if (!isset($data['id']) || !$data['id']) {
            throw new Error(500, '参数异常');
        }
        $params = [
            'index' => $index,
            'type' => $type,
            'id' => $data['id'],
            'body' => $data
        ];
        return $this->esClient->index($params);
    }

    /**
     * 修改数据
     * @param $index
     * @param $type
     * @param $data
     * @throws Error
     */
    public function update($index, $type, $data)
    {
        if (!isset($data['id']) || !$data['id']) {
            throw new Error(500, '参数异常');
        }
        $params = [
            'index' => $index,
            'type' => $type,
            'id' => $data['id'],
            'body' => [
                'doc' => $data
            ]
        ];
        return $client->update($params);
    }

    /**
     * 批量处理数据
     * @param $index
     * @param $type
     * @param $data
     * @return array
     * @throws Error
     */
    public function batch($index, $type, $data)
    {
        $params = [];
        foreach ($data as $item) {
            if (!isset($item['id']) || !$item['id']) {
                throw new Error(500, '参数异常');
            }
            $params['body'][] = [
                'index' => [
                    '_index' => $index,
                    '_type' => $type,
                    '_id' => $item['id']
                ]
            ];
            $params['body'][] = $item;
        }
        return $this->esClient->bulk($params);
    }
}
