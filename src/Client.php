<?php

namespace Retrinko\RabbitMQ\Admin;

use Psr\Log\LoggerAwareTrait;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Retrinko\RabbitMQ\Admin\Exceptions\Exception;
use Retrinko\Scylla\Response\Factories\JsonResponsesFactory;
use Retrinko\Scylla\Response\ResponseInterface;
use Retrinko\Scylla\Client as HttpClient;
use Retrinko\Scylla\Request\RequestInterface;
use Retrinko\Scylla\Request\Requests\JsonRequest;
use Retrinko\Scylla\Util\HttpCodes;
use Retrinko\UrlComposer\UrlComposer;


class Client
{
    use LoggerAwareTrait;

    /**
     * @var HttpClient
     */
    protected $httpClient;
    /**
     * @var string
     */
    protected $user;
    /**
     * @var string
     */
    protected $pass;
    /**
     * @var string
     */
    protected $apiUrl;

    /**
     * @param string $apiUrl
     * @param string $user
     * @param string $pass
     */
    public function __construct($apiUrl, $user, $pass)
    {
        $this->logger = new NullLogger();
        $this->httpClient = new HttpClient();
        $this->httpClient->setLogger($this->logger);
        $this->httpClient->setResponsesFactory(new JsonResponsesFactory());

        $this->apiUrl = $apiUrl;
        $this->user = $user;
        $this->pass = $pass;
    }

    /**
     * @param LoggerInterface $logger
     */
    public function setLogger(LoggerInterface $logger)
    {
        $this->logger = $logger;
        $this->httpClient->setLogger($logger);
    }

    /**
     * @param RequestInterface $request
     *
     * @return ResponseInterface
     * @throws Exception
     * @throws \Retrinko\Scylla\Exceptions\Exception
     */
    protected function executeRequest(RequestInterface $request)
    {
        $loggerEnv['request-method'] = $request->getRequestMethod();
        $loggerEnv['request-url'] = $request->getUrl();
        $loggerEnv['request-params'] = $request->getParams();

        // Execute request
        /** @var ResponseInterface $response */
        $response = $this->httpClient->exec($request)->current();

        // Add info to loggerEnv
        $loggerEnv['code'] = $response->getCode();
        $loggerEnv['msg'] = $response->getMessage();
        $loggerEnv['content'] = $response->getContent();

        // Check response code
        $code = $response->getCode();
        if (false === HttpCodes::isError($code))
        {
            $this->logger->notice('Request execution success!', $loggerEnv);
        }
        else
        {
            $this->logger->error('Error executing request!', $loggerEnv);
            throw new Exception(sprintf('Error executing request [%s] %s (%s): %s',
                                        $request->getRequestMethod(),
                                        $request->getUrl(),
                                        implode(', ', $request->getParams()),
                                        $response->getMessage()));
        }

        return $response;
    }

    /**
     * @param string $name
     * @param string $passwd
     * @param array $tags
     *
     * @return bool
     * @throws Exception
     * @throws \Retrinko\Scylla\Exceptions\Exception
     * @throws \Retrinko\UrlComposer\Exceptions\UrlException
     */
    public function createUser($name, $passwd, $tags = [])
    {
        $tagsStr = implode(',', $tags);

        // Build URL: /users/$name
        $url = new UrlComposer($this->apiUrl);
        $url->addToPath('users')->addToPath($name);

        // Build request
        $request = new JsonRequest($url->__toString(), RequestInterface::REQUEST_METHOD_PUT);
        $request->setAuth($this->user, $this->pass);
        $request->addParam('password', $passwd)->addParam('tags', $tagsStr);

        // Execute request
        $this->executeRequest($request);

        return true;
    }

    /**
     * @param string $name
     *
     * @return bool
     * @throws Exception
     * @throws \Retrinko\Scylla\Exceptions\Exception
     * @throws \Retrinko\UrlComposer\Exceptions\UrlException
     */
    public function deleteUser($name)
    {
        // Build URL: /users/$name
        $url = new UrlComposer($this->apiUrl);
        $url->addToPath('users')->addToPath($name);

        // Build request
        $request = new JsonRequest($url->__toString(), RequestInterface::REQUEST_METHOD_DELETE);
        $request->setAuth($this->user, $this->pass);

        // Execute request
        $this->executeRequest($request);

        return true;
    }


    /**
     * @param string $name
     *
     * @return array
     * @throws Exception
     * @throws \Retrinko\Scylla\Exceptions\Exception
     * @throws \Retrinko\UrlComposer\Exceptions\UrlException
     */
    public function getUser($name)
    {
        // Build URL: /users/$name
        $url = new UrlComposer($this->apiUrl);
        $url->addToPath('users')->addToPath($name);

        // Build request
        $request = new JsonRequest($url->__toString(), RequestInterface::REQUEST_METHOD_GET);
        $request->setAuth($this->user, $this->pass);

        // Execute request
        $response = $this->executeRequest($request);

        return $response->getDecodedContent();
    }

    /**
     * @param string $name
     *
     * @return boolean
     */
    public function userExist($name)
    {
        try
        {
            $this->getUser($name);
            $exist = true;
        }
        catch (\Exception $e)
        {
            $exist = false;
        }

        return $exist;
    }

    /**
     * @return array
     * @throws Exception
     * @throws \Retrinko\Scylla\Exceptions\Exception
     * @throws \Retrinko\UrlComposer\Exceptions\UrlException
     */
    public function getUsers()
    {
        // Build URL: /users
        $url = new UrlComposer($this->apiUrl);
        $url->addToPath('users');

        // Build request
        $request = new JsonRequest($url->__toString(), RequestInterface::REQUEST_METHOD_GET);
        $request->setAuth($this->user, $this->pass);

        // Execute request
        $response = $this->executeRequest($request);

        return $response->getDecodedContent();
    }


    /**
     * @param string $name
     * @param string $vhost
     * @param string $config
     * @param string $write
     * @param string $read
     *
     * @return bool
     * @throws Exception
     * @throws \Retrinko\Scylla\Exceptions\Exception
     * @throws \Retrinko\UrlComposer\Exceptions\UrlException
     */
    public function setUserPermissions($name, $vhost = '%2f', $config = '', $write = '', $read = '')
    {
        // Build URL: /permissions/$vhost/$name
        $url = new UrlComposer($this->apiUrl);
        $url->addToPath('permissions')->addToPath($vhost)->addToPath($name);

        // Build request
        $request = new JsonRequest($url->__toString(), RequestInterface::REQUEST_METHOD_PUT);
        $request->setAuth($this->user, $this->pass);
        $request->addParam('configure', $config)
                ->addParam('write', $write)
                ->addParam('read', $read);

        // Execute request
        /** @var ResponseInterface $response */
        $response = $this->httpClient->exec($request)->current();
        $loggerEnv['code'] = $response->getCode();
        $loggerEnv['msg'] = $response->getMessage();

        // Check response code
        $this->executeRequest($request);

        return true;
    }


    /**
     * @param string $name
     * @param string $vhost
     *
     * @return bool
     * @throws Exception
     * @throws \Retrinko\Scylla\Exceptions\Exception
     * @throws \Retrinko\UrlComposer\Exceptions\UrlException
     */
    public function createQueue($name, $vhost = '%2f')
    {
        // Build URL: /queues/$vhost/$name
        $url = new UrlComposer($this->apiUrl);
        $url->addToPath('queues')->addToPath($vhost)->addToPath($name);

        // Build request
        $request = new JsonRequest($url->__toString(), RequestInterface::REQUEST_METHOD_PUT);
        $request->setAuth($this->user, $this->pass);
        $request->addParam('auto_delete', false)
                ->addParam('durable', true)
                ->addParam('arguments', []);

        // Execute request
        $this->executeRequest($request);

        return true;
    }

    /**
     * @param string $name
     * @param string $vhost
     *
     * @return bool
     * @throws Exception
     * @throws \Retrinko\Scylla\Exceptions\Exception
     * @throws \Retrinko\UrlComposer\Exceptions\UrlException
     */
    public function deleteQueue($name, $vhost = '%2f')
    {
        // Build URL: /queues/$vhost/$name
        $url = new UrlComposer($this->apiUrl);
        $url->addToPath('queues')->addToPath($vhost)->addToPath($name);

        // Build request
        $request = new JsonRequest($url->__toString(), RequestInterface::REQUEST_METHOD_DELETE);
        $request->setAuth($this->user, $this->pass);

        // Execute request
        $this->executeRequest($request);

        return true;
    }

    /**
     * @return array
     * @throws Exception
     * @throws \Retrinko\Scylla\Exceptions\Exception
     * @throws \Retrinko\UrlComposer\Exceptions\UrlException
     */
    public function getQueues()
    {
        // Build URL: /queues
        $url = new UrlComposer($this->apiUrl);
        $url->addToPath('queues');

        // Build request
        $request = new JsonRequest($url->__toString(), RequestInterface::REQUEST_METHOD_GET);
        $request->setAuth($this->user, $this->pass);

        // Execute request
        $response = $this->executeRequest($request);

        return $response->getDecodedContent();
    }

    /**
     *
     * @param string $exchangeName
     * @param string $queueName
     * @param string $routingKey
     * @param string $vhost
     * @param array $args
     *
     * @return bool
     * @throws Exception
     * @throws \Retrinko\Scylla\Exceptions\Exception
     * @throws \Retrinko\UrlComposer\Exceptions\UrlException
     */
    public function createBinding($exchangeName, $queueName, $routingKey = null, $vhost = '%2f',
                                  $args = [])
    {
        // Build URL: /bindings/$vhost/e/$exchangeName/q/$queueName
        $url = new UrlComposer($this->apiUrl);
        $url->addToPath('bindings')
            ->addToPath($vhost)
            ->addToPath('e')
            ->addToPath($exchangeName)
            ->addToPath('q')
            ->addToPath($queueName);;

        // Build request
        $request = new JsonRequest($url->__toString(), RequestInterface::REQUEST_METHOD_POST);
        $request->setAuth($this->user, $this->pass);
        $request->addParam('arguments', $args);
        if (!is_null($routingKey))
        {
            $request->addParam('routing_key', $routingKey);
        }

        // Execute request
        $this->executeRequest($request);

        return true;
    }

    /**
     * @return array
     * @throws Exception
     * @throws \Retrinko\Scylla\Exceptions\Exception
     * @throws \Retrinko\UrlComposer\Exceptions\UrlException
     */
    public function getOverview()
    {
        // Build URL: /overview
        $url = new UrlComposer($this->apiUrl);
        $url->addToPath('overview');

        // Build request
        $request = new JsonRequest($url->__toString(), RequestInterface::REQUEST_METHOD_GET);
        $request->setAuth($this->user, $this->pass);

        // Execute request
        $response = $this->executeRequest($request);

        return $response->getDecodedContent();
    }

    /**
     * @return array
     * @throws Exception
     * @throws \Retrinko\Scylla\Exceptions\Exception
     * @throws \Retrinko\UrlComposer\Exceptions\UrlException
     */
    public function getNodes()
    {
        // Build URL: /nodes
        $url = new UrlComposer($this->apiUrl);
        $url->addToPath('nodes');

        // Build request
        $request = new JsonRequest($url->__toString(), RequestInterface::REQUEST_METHOD_GET);
        $request->setAuth($this->user, $this->pass);

        // Execute request
        $response = $this->executeRequest($request);

        return $response->getDecodedContent();
    }

    /**
     * @param string $name
     * @param bool $includeMemoryStatistics
     *
     * @return array
     * @throws Exception
     * @throws \Retrinko\Scylla\Exceptions\Exception
     * @throws \Retrinko\UrlComposer\Exceptions\UrlException
     */
    public function getNode($name, $includeMemoryStatistics = false)
    {
        // Build URL: /nodes[?memory=true]
        $url = new UrlComposer($this->apiUrl);
        $url->addToPath('nodes')->addToPath($name);
        if ($includeMemoryStatistics)
        {
            $url->addToQuery('memory', 'true');
        }

        // Build request
        $request = new JsonRequest($url->__toString(), RequestInterface::REQUEST_METHOD_GET);
        $request->setAuth($this->user, $this->pass);

        // Execute request
        $response = $this->executeRequest($request);

        return $response->getDecodedContent();
    }
    
}