<?php

class ReadUsersCommand extends CConsoleCommand
{
    const FILE_1 = 'protected/data/users_file1.json';
    const FILE_2 = 'protected/data/users_file2.json';
    const KAFKA_BROKER = 'localhost:9092';
    const KAFKA_TOPIC = 'users_topic';

    private $kafkaProducer;

    public function actionIndex()
    {
        $startTime = microtime(true);
        $processedCount = 0;

        // Настраиваем Kafka Producer
        $this->setupKafkaProducer();

        // Открываем файлы
        $file1 = fopen(self::FILE_1, 'r');
        $file2 = fopen(self::FILE_2, 'r');

        if ($file1 === false || $file2 === false) {
            echo "Не удалось открыть файлы.";
            return;
        }

        // Перемещаем указатель за скобку открытия массива
        fseek($file1, 1);
        fseek($file2, 1);

        $endOfFile1 = false;
        $endOfFile2 = false;

        while (!$endOfFile1 || !$endOfFile2) {

            if (!$endOfFile1) {
                $object1 = $this->readNextJsonObject($file1);

                if ($object1 !== null) {
                    $this->sendToKafka($object1);
                    $processedCount++;
                } else {
                    $endOfFile1 = true;
                }
            }

            if (!$endOfFile2) {
                $object2 = $this->readNextJsonObject($file2);

                if ($object2 !== null) {
                    $this->sendToKafka($object2);
                    $processedCount++;
                } else {
                    $endOfFile2 = true;
                }
            }
        }

        fclose($file1);
        fclose($file2);

        $executionTime = microtime(true) - $startTime;
        $memoryUsage = memory_get_peak_usage(true) / (1024 * 1024);

        echo "Время выполнения: $executionTime секунд\n";
        echo "Потребление памяти: $memoryUsage Мб\n";
        echo "Количество обработанных объектов в секунду: " . ($processedCount / $executionTime) . "\n";
    }

    private function readNextJsonObject($fileHandle)
    {
        while (($char = fgetc($fileHandle)) !== false) {

            if ($char === '{') {
                $buffer = $char;

                while (($char = fgetc($fileHandle)) !== false) {
                    $buffer .= $char;

                    if ($char === '}') {
                        // Удаляем запятую, если есть
                        $nextChar = fgetc($fileHandle);

                        if ($nextChar !== false && $nextChar !== ']') {
                            fseek($fileHandle, -1, SEEK_CUR);
                        }

                        return json_decode($buffer, true);
                    }
                }
            }
        }

        return null;
    }

    private function setupKafkaProducer()
    {
        $conf = new RdKafka\Conf();
        $conf->set('metadata.broker.list', self::KAFKA_BROKER);
        $this->kafkaProducer = new RdKafka\Producer($conf);
    }

    private function sendToKafka($object)
    {
        $topic = $this->kafkaProducer->newTopic(self::KAFKA_TOPIC);
        $message = json_encode($object);
        $topic->produce(RD_KAFKA_PARTITION_UA, 0, $message);
        $this->kafkaProducer->poll(0);
    }
}
