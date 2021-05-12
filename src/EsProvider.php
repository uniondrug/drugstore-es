<?php
/**
 * Created by PhpStorm.
 * User: lvchaohui
 * Date: 2021/5/8
 * Time: 6:13 PM
 */
namespace Uniondrug\DrugstoreEs;

use Elasticsearch\ClientBuilder;
use Phalcon\Di\ServiceProviderInterface;
use Uniondrug\Framework\Services\ServiceTrait;

/**
 * Class EsProvider
 * @package Uniondrug\DrugstoreEs
 */
class EsProvider implements ServiceProviderInterface
{
    use ServiceTrait;

    public function register(\Phalcon\DiInterface $di)
    {
        $config = config()->path('elasticsearch');
        $di->set('esClient', function() use ($config, $di){
            // 获取es对象
            $client = ClientBuilder::create()->setHosts([$config->host])->setBasicAuthentication($config->user, $config->pass)->build();
            return $client;
        });
        // 服务service
        $di->set('esService', function(){
            return new EsService();
        });
    }
}