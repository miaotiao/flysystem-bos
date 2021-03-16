# laravel-flysystem-bos
Laravel Flysystem adapter for Baidu Bos

##  Usage
config/filesystems.php

```
'bos' => [
    'driver'  => 'bos',
    'bucket'  => env('BOS_BUCKET', ''),
    //  bos 根目录文件夹
    'prefix'  => env('BOS_PREFIX', ''),
    'options' => [
        'credentials' => [
            'accessKeyId'     => env('BOS_AK', ''),
            'secretAccessKey' => env('BOS_SK', ''),
        ],
        'endpoint'    => 'bj.bcebos.com',
        'protocol'    => 'https',
    ],
],
```



