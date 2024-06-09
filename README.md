Для того чтобы выполнить данное задание нужно установить Kafka и Zookeeper.
Затем мы устанавливаем PHP клиент php-rdkafka для Kafka и добавляем расширение в наш файл php.ini: extension=rdkafka.so
Запускаем Zookeeper: bin/zookeeper-server-start.sh config/zookeeper.properties
Запускаем в другой вкладке Kafka: bin/kafka-server-start.sh config/server.properties

Два JSON-файла уже добавлены мной в testdrive/protected/data

Запуск осуществляется в корне папки testdrive командой: protected/yiic readUsers

Получившиеся результаты можно увидеть в консоли после запуска Kafka и Zookeeper при помощи команды:
bin/kafka-console-consumer.sh --topic users_topic --bootstrap-server localhost:9092

Сам лог выполнения будет предоставлен после выполнения команды protected/yiic readUsers
