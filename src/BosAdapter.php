<?php
/**
 *
 * @Author iwill
 * @Datetime 2021/3/9 17:51
 */

namespace Miaotiao\Flysystem\BaiduBos;

use BaiduBce\Auth\SignOptions;
use BaiduBce\Log\LogFactory;
use BaiduBce\Services\Bos\BosOptions;
use baidubce\util\HashUtils;
use League\Flysystem\Adapter\AbstractAdapter;
use League\Flysystem\Adapter\Polyfill\NotSupportingVisibilityTrait;
use League\Flysystem\Config;
use League\Flysystem\Util;

class BosAdapter extends AbstractAdapter
{
    use NotSupportingVisibilityTrait;

    protected static $resultMap = [
        BosOptions::CONTENT_LENGTH => 'size',
        BosOptions::CONTENT_TYPE   => 'mimetype',
        BosOptions::ETAG           => 'etag',
        BosOptions::CONTENT_MD5    => 'contentMd5',
        BosOptions::DATE           => 'date',
        BosOptions::LAST_MODIFIED  => 'lastModified',
        BosOptions::USER_METADATA  => 'userMetadata',
    ];

    protected static $metaOptions = [
        BosOptions::CONTENT_LENGTH,
        BosOptions::CONTENT_TYPE,
        BosOptions::CONTENT_MD5,
        BosOptions::CONTENT_SHA256,
        BosOptions::USER_METADATA,
    ];
    protected $client, $bucket, $logger;

    public function __construct($client = '', $bucket = '', string $prefix = '')
    {
        $this->client = $client;
        $this->bucket = $bucket;
        $this->logger = LogFactory::getLogger('BaiduBos');
        $this->setPathPrefix($prefix);
    }

    /**
     * 可修改 bucket
     * @param null $bucket
     */
    public function setBucket($bucket = null)
    {
        if (!empty($bucket)) {
            $this->bucket = $bucket;
        }
    }

    public function write($path, $contents, Config $config)
    {
        return $this->upload($path, $contents, $config);
    }

    /**
     * 实际上传
     * @param $path
     * @param $contents
     * @param Config $config
     */
    protected function upload($path, $contents, Config $config)
    {
        $options  = $this->getOptionsFromConfig($config);
        $response = null;
        $path     = $this->applyPathPrefix($path);

        try {
            if (is_string($contents)) {
                $response = $this->client->putObjectFromString($this->bucket, $path, $contents, $options);
            } else if (is_resource($contents)) {
                if (isset($options['file'])) {
                    $file = $options['file'];
                } else {
                    $metadata = stream_get_meta_data($contents);
                    $file     = $metadata['uri'];
                }

                if ($file !== null) {
                    $response = $this->client->putObjectFromFile($this->bucket, $path, $file, $options);
                } else {
                    if (!isset($options[BosOptions::CONTENT_TYPE])) {
                    }

                    if (!isset($options[BosOptions::CONTENT_LENGTH])) {
                        $contentLength = Util::getStreamSize($contents);
                    } else {
                        $contentLength = $options[BosOptions::CONTENT_LENGTH];
                        unset($options[BosOptions::CONTENT_LENGTH]);
                    }

                    if (!isset($options[BosOptions::CONTENT_MD5])) {
                        $contentMd5 = base64_encode(HashUtils::md5FromStream($contents, 0, $contentLength));
                    } else {
                        $contentMd5 = $options[BosOptions::CONTENT_MD5];
                        unset($options[BosOptions::CONTENT_MD5]);
                    }

                    $response = $this->client->putObject($this->bucket, $path, $contents, $contentLength, $contentMd5, $options);
                    if (is_resource($contents)) {
                        fclose($contents);
                    }
                }
            } else {
                throw new \InvalidArgumentException("$contents type should be string or resource");
            }
        } catch (BceBaseException $e) {
            if (stcmp(gettype($e), "BceClientException") == 0) {
                $this->logger->debug("BceClientException: " . $e->getMessage());
            }
            if (stcmp(gettype($e), "BceServerException") == 0) {
                $this->logger->debug("BceServerException: " . $e->getMessage());
            }
            if (is_resource($contents)) {
                fclose($contents);
            }
            return false;
        } catch (\InvalidArgumentException $e) {
            $this->logger->debug("InvalidArgumentException: " . $e->getMessage());
            if (is_resource($contents)) {
                fclose($contents);
            }
            return false;
        } catch (\Exception $e) {
            $this->logger->debug("Exception: " . $e->getMessage());
            if (is_resource($contents)) {
                fclose($contents);
            }
            return false;
        }
        return $this->normalizeResponse($response->metadata, $path);
    }

    /**
     * 从配置中获取配置信息
     * @param Config $config
     * @return array
     */
    protected function getOptionsFromConfig(Config $config)
    {
        $options = [];

        if ($mimetype = $config->get('mimetype')) {
            $options['mimetype']               = $mimetype;
            $options[BosOptions::CONTENT_TYPE] = $mimetype;
        }

        foreach (static::$metaOptions as $option) {
            if ($config->has($option)) {
                $options[$option] = $config->get($option);
            }
        }

        return $options;
    }

    /**
     * 返回响应
     * @param array $response
     * @param null $path
     * @return array|null[]
     */
    protected function normalizeResponse(array $response, $path = null)
    {
        $result = ['path' => $path ?: $this->removePathPrefix(null)];

        if (isset($response['date'])) {
            $result['timestamp'] = $response['date']->getTimestamp();
        }

        if (isset($response['lastModified'])) {
            $result['timestamp'] = $response['lastModified']->getTimestamp();
        }

        if (substr($result['path'], -1) === '/') {
            $result['type'] = 'dir';
            $result['path'] = rtrim($result['path'], '/');

            return $result;
        }

        return array_merge($result, Util::map($response, static::$resultMap), ['type' => 'file']);
    }

    public function writeStream($path, $resource, Config $config)
    {
        return $this->upload($path, $resource, $config);
    }

    public function update($path, $contents, Config $config)
    {
        return $this->upload($path, $contents, $config);
    }

    public function updateStream($path, $resource, Config $config)
    {
        return $this->upload($path, $resource, $config);
    }

    public function rename($path, $newpath)
    {
        if (!$this->copy($path, $newpath)) {
            return false;
        }

        return $this->delete($path);
    }

    public function copy($path, $newpath)
    {
        $path = $this->applyPathPrefix($path);
        try {
            $this->client->copyObject($this->bucket, $path, $this->bucket, $newpath);
        } catch (\Exception $e) {
            $this->logger->debug(gettype($e) . ": " . $e->getMessage());
            return false;
        }
        return true;
    }

    public function delete($path)
    {
        $path = $this->applyPathPrefix($path);
        try {
            $this->client->deleteObject($this->bucket, $path);
        } catch (\Exception $e) {
            $this->logger->debug(gettype($e) . ": " . $e->getMessage());
            return false;
        }
        return true;
    }

    public function deleteDir($dirname)
    {
        $objects = $this->listContents($dirname, true);
        try {
            foreach ($objects as $object) {
                $this->client->deleteObject($this->bucket, $object['path']);
            }
            $dirname = $dirname . $this->pathSeparator;
            $this->client->deleteObject($this->bucket, $dirname);
        } catch (\Exception $e) {
            $this->logger->debug(gettype($e) . ": " . $e->getMessage());
            return false;
        }
        return true;
    }

    public function listContents($directory = '', $recursive = false)
    {
        $result    = [];
        $prefix    = empty($directory) ? $directory : ($directory . '/');
        $delimiter = $recursive ? '' : '/';
        $marker    = '';
        $options   = array(
//            BosOptions::MAX_KEYS => 1000,
            BosOptions::PREFIX    => $prefix,
//            BosOptions::MARKER => $marker,
            BosOptions::DELIMITER => $delimiter,
        );
        $directory = $this->applyPathPrefix($directory);

        try {
            while (true) {
                if ($marker !== null) {
                    $options[BosOptions::MARKER] = $marker;
                }
                $response = $this->client->listObjects($this->bucket, $options);
                foreach ($response->contents as $object) {
                    if ($object->key != $prefix)
                        $type = (substr($object->key, -1) == $this->pathSeparator) ? 'dir' : 'file';
                    $result[] = [
                        'timestamp' => $object->lastModified,
                        'type'      => $type,
                        'path'      => $object->key,
                        'size'      => $object->size,
                        'etag'      => $object->eTag,
                    ];
                }
                if (isset($response->commonPrefixes)) {
                    foreach ($response->commonPrefixes as $object) {
                        $result[] = [
                            'type' => 'dir',
                            'path' => $object->prefix,
                        ];
                    }
                }
                if ($response->isTruncated) {
                    $marker = $response->nextMarker;
                } else {
                    break;
                }
            }
        } catch (\Exception $e) {
            $this->logger->debug(gettype($e) . ": " . $e->getMessage());
            return [];
        }
        return $result;
    }

    public function createDir($dirname, Config $config)
    {
        $dirname = rtrim($dirname, '/') . '/';
        $dirname = $this->applyPathPrefix($dirname);

        try {
            $result = $this->client->putObjectFromString($this->bucket, $dirname, '');
            if (!$result) {
                return false;
            }
        } catch (\Exception $e) {
            $this->logger->debug(gettype($e) . ": " . $e->getMessage());
            return false;
        }

        return ['path' => $dirname, 'type' => 'dir'];
    }

    public function has($path)
    {
        $path = $this->applyPathPrefix($path);

        try {
            $this->client->getObjectMetadata($this->bucket, $path);
        } catch (\Exception $e) {
            $this->logger->debug(gettype($e) . ": " . $e->getMessage());
            return false;
        }
        return true;
    }

    public function read($path)
    {
        $path = $this->applyPathPrefix($path);

        try {
            $contents = $this->client->getObjectAsString($this->bucket, $path);
        } catch (\Exception $e) {
            $this->logger->debug(gettype($e) . ": " . $e->getMessage());
            return false;
        }
        return compact('contents', 'path');
    }

    public function readStream($path)
    {
        $path = $this->applyPathPrefix($path);

        try {
            $outputStream = fopen('php://memory', 'r+');
            $response     = $this->client->getObject($this->bucket, $path, $outputStream);
            rewind($outputStream);
            unset($response);
            $response['stream'] = $outputStream;
        } catch (\Exception $e) {
            $this->logger->debug(gettype($e) . ": " . $e->getMessage());
            return false;
        }
        return $response;
    }

    public function getSize($path)
    {
        return $this->getMetadata($path);
    }

    /**
     * 获取文件的 meta 信息
     * @param string $path
     * @return array|false|null[]
     */
    public function getMetadata($path)
    {
        $path = $this->applyPathPrefix($path);

        try {
            $result = $this->client->getObjectMetadata($this->bucket, $path);
        } catch (\Exception $e) {
            $this->logger->debug(gettype($e) . ": " . $e->getMessage());
            return false;
        }
        return $this->normalizeResponse($result, $path);
    }

    public function getMimetype($path)
    {
        return $this->getMetadata($path);
    }

    public function getTimestamp($path)
    {
        return $this->getMetadata($path);
    }

    /**
     * 获取链接
     * @param $path
     * @return false|string
     */
    public function getUrl($path)
    {
        return $this->getTemporaryLink($path, 600);
    }

    /**
     * 获取暂时的地址
     * @param $path
     */
    public function getTemporaryLink($path, $expire = 600)
    {
        $path        = $this->applyPathPrefix($path);
        $signOptions = [
            SignOptions::TIMESTAMP             => new \DateTime(),
            SignOptions::EXPIRATION_IN_SECONDS => $expire,
        ];

        try {
            $url = $this->client->generatePreSignedUrl($this->bucket, $path, [BosOptions::SIGN_OPTIONS => $signOptions]);
        } catch (\Exception $e) {
            $this->logger->debug(gettype($e) . ": " . $e->getMessage());
            return false;
        }
        return $url;
    }

}