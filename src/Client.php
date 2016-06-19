<?php

namespace Retrinko\RabbitMQ\Admin;

use Psr\Log\LoggerAwareTrait;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Retrinko\RabbitMQ\Admin\Exceptions\Exception;
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
        $loggerEnv = get_defined_vars();
        $tagsStr = implode(',', $tags);

        // Build URL: /users/$name
        $url = new UrlComposer($this->apiUrl);
        $url->addToPath('users')->addToPath($name);

        // Build request
        $request = new JsonRequest($url->__toString(), RequestInterface::REQUEST_METHOD_PUT);
        $request->setAuth($this->user, $this->pass);
        $request->addParam('password', $passwd)->addParam('tags', $tagsStr);

        // Execute request
        /** @var ResponseInterface $response */
        $response = $this->httpClient->exec($request)->current();
        $loggerEnv['code'] = $response->getCode();
        $loggerEnv['msg'] = $response->getMessage();

        // Check response code
        $code = $response->getCode();
        if (false === HttpCodes::isError($code))
        {
            $this->logger->notice('User created!', $loggerEnv);
        }
        else
        {
            $this->logger->error('Error creating user!', $loggerEnv);
            throw new Exception(sprintf('Error creating user "%s": (%s) %s',
                                        $name,
                                        $response->getCode(),
                                        $response->getMessage()));
        }

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
        $loggerEnv = get_defined_vars();

        // Build URL: /users/$name
        $url = new UrlComposer($this->apiUrl);
        $url->addToPath('users')->addToPath($name);

        // Build request
        $request = new JsonRequest($url->__toString(), RequestInterface::REQUEST_METHOD_DELETE);
        $request->setAuth($this->user, $this->pass);

        // Execute request
        /** @var ResponseInterface $response */
        $response = $this->httpClient->exec($request)->current();
        $loggerEnv['code'] = $response->getCode();
        $loggerEnv['msg'] = $response->getMessage();

        // Check response code
        $code = $response->getCode();
        if (false === HttpCodes::isError($code))
        {
            $this->logger->notice('User deleted!', $loggerEnv);
        }
        else
        {
            $this->logger->error('Error deleting user!', $loggerEnv);
            throw new Exception(sprintf('Error deleting user "%s": (%s) %s',
                                        $name,
                                        $response->getCode(),
                                        $response->getMessage()));
        }

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
        $loggerEnv = get_defined_vars();

        // Build URL: /users/$name
        $url = new UrlComposer($this->apiUrl);
        $url->addToPath('users')->addToPath($name);

        // Build request
        $request = new JsonRequest($url->__toString(), RequestInterface::REQUEST_METHOD_GET);
        $request->setAuth($this->user, $this->pass);

        // Execute request
        /** @var ResponseInterface $response */
        $response = $this->httpClient->exec($request)->current();
        $loggerEnv['code'] = $response->getCode();
        $loggerEnv['msg'] = $response->getMessage();

        // Check response code
        $code = $response->getCode();
        if (false === HttpCodes::isError($code))
        {
            $this->logger->notice('User loaded!', $loggerEnv);
        }
        else
        {
            $this->logger->error('Error loading user!', $loggerEnv);
            throw new Exception(sprintf('Error loading user "%s": (%s) %s',
                                        $name,
                                        $response->getCode(),
                                        $response->getMessage()));
        }

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
        $loggerEnv = get_defined_vars();

        // Build URL: /users
        $url = new UrlComposer($this->apiUrl);
        $url->addToPath('users');

        // Build request
        $request = new JsonRequest($url->__toString(), RequestInterface::REQUEST_METHOD_GET);
        $request->setAuth($this->user, $this->pass);

        // Execute request
        /** @var ResponseInterface $response */
        $response = $this->httpClient->exec($request)->current();
        $loggerEnv['code'] = $response->getCode();
        $loggerEnv['msg'] = $response->getMessage();

        // Check response code
        $code = $response->getCode();
        if (false === HttpCodes::isError($code))
        {
            $this->logger->notice('Users obtained!', $loggerEnv);
        }
        else
        {
            $this->logger->error('Error getting users!', $loggerEnv);
            throw new Exception(sprintf('Error getting users: (%s ) %s',
                                        $response->getCode(),
                                        $response->getMessage()));
        }

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
        $loggerEnv = get_defined_vars();

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
        $code = $response->getCode();
        if (false === HttpCodes::isError($code))
        {
            $this->logger->notice('User permission setted!', $loggerEnv);
        }
        else
        {
            $this->logger->error('Error setting user permissions!', $loggerEnv);
            throw new Exception(sprintf('Error setting user permissions for "%s": (%s) %s',
                                        $name,
                                        $response->getCode(),
                                        $response->getMessage()));
        }

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
        $loggerEnv = get_defined_vars();

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
        /** @var ResponseInterface $response */
        $response = $this->httpClient->exec($request)->current();
        $loggerEnv['code'] = $response->getCode();
        $loggerEnv['msg'] = $response->getMessage();

        // Check response code
        $code = $response->getCode();
        if (false === HttpCodes::isError($code))
        {
            $this->logger->notice('Queue created!', $loggerEnv);
        }
        else
        {
            $this->logger->error('Error creating queue!', $loggerEnv);
            throw new Exception(sprintf('Error creating queue "%s": (%s) %s',
                                        $name,
                                        $response->getCode(),
                                        $response->getMessage()));
        }

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
        $loggerEnv = get_defined_vars();

        // Build URL: /queues/$vhost/$name
        $url = new UrlComposer($this->apiUrl);
        $url->addToPath('queues')->addToPath($vhost)->addToPath($name);

        // Build request
        $request = new JsonRequest($url->__toString(), RequestInterface::REQUEST_METHOD_DELETE);
        $request->setAuth($this->user, $this->pass);

        // Execute request
        /** @var ResponseInterface $response */
        $response = $this->httpClient->exec($request)->current();
        $loggerEnv['code'] = $response->getCode();
        $loggerEnv['msg'] = $response->getMessage();

        // Check response code
        $code = $response->getCode();
        if (false === HttpCodes::isError($code))
        {
            $this->logger->notice('Queue deleted!', $loggerEnv);
        }
        else
        {
            $this->logger->error('Error deleting queue!', $loggerEnv);
            throw new Exception(sprintf('Error deleting queue "%s": (%s) %s',
                                        $name,
                                        $response->getCode(),
                                        $response->getMessage()));
        }

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
        $loggerEnv = get_defined_vars();

        // Build URL: /queues
        $url = new UrlComposer($this->apiUrl);
        $url->addToPath('queues');

        // Build request
        $request = new JsonRequest($url->__toString(), RequestInterface::REQUEST_METHOD_GET);
        $request->setAuth($this->user, $this->pass);

        // Execute request
        /** @var ResponseInterface $response */
        $response = $this->httpClient->exec($request)->current();
        $loggerEnv['code'] = $response->getCode();
        $loggerEnv['msg'] = $response->getMessage();

        // Check response code
        $code = $response->getCode();
        if (false === HttpCodes::isError($code))
        {
            $this->logger->notice('Queue obteined!', $loggerEnv);
        }
        else
        {
            $this->logger->error('Error getting queues!', $loggerEnv);
            throw new Exception(sprintf('Error getting queues: (%s) %s',
                                        $response->getCode(),
                                        $response->getMessage()));
        }

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
        $loggerEnv = get_defined_vars();

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
        /** @var ResponseInterface $response */
        $response = $this->httpClient->exec($request)->current();
        $loggerEnv['code'] = $response->getCode();
        $loggerEnv['msg'] = $response->getMessage();

        // Check response code
        $code = $response->getCode();
        if (false === HttpCodes::isError($code))
        {
            $this->logger->notice('Binding created!', $loggerEnv);
        }
        else
        {
            $this->logger->error('Error creating binding!', $loggerEnv);
            throw new Exception(sprintf('Error creating binding: (%s) %s',
                                        $response->getCode(),
                                        $response->getMessage()));
        }

        return true;
    }

}