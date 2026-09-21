# Informe

## Coordinación

### Cliente
Se utiliza un client_id generado en el handler para poder reconocer a un cliente de otro. Esto es necesario para poder reconocer a que cliente enviar la información y como diferenciar a uno de otro. 

### Sum
Por un lado, los Sum pueden comunicarse entre replicas para poder avisar cuando uno de ellos reciba el EOF de un cliente. De esta manera todos se enteran que para un client_id determinado, se recibió toda la información disponible, incluso estando dispersado entre diferentes replicas del Sum. De esta forma despues, el Sum envía su información a una réplica del Aggregator. 
Para la forma de distribuir que fruta va a cada Aggregator en la replica de Sum, se utiliza el método matching_aggregator_fruit, que hace un cálculo con la función ord() y en base a eso determina que réplica del Aggregator va a recibir la información de esa fruta para procesarla. De esta forma se hace una distribución sobre las frutas para que se procesen, aproximadamente, de forma equitativa entre las distintas replicas del Aggregator, enviando la información sobre esa fruta siempre a la misma instancia del Aggregator.
Para comunicarse estas replicas de Sum, se utiliza una queue que se genera con los nombres de las instancias que se reciben por las variables de entorno seteadas en el docker-compose, permitiendo que cambien los nombres de los servicios y funcionando de igual manera. 
La instancia de Sum que reciba el EOF de un cliente envía a todas las queues (incluida la suya) de las instancias de Sum que ese cliente terminó de enviar información, y esto se lee en un hilo que va por fuera del hilo principal de Sum, y solo espera a que lleguen estos mensajes para procesarlos.

### Aggregator
El Aggregator va recibiendo, para un client_id, la información sobre ciertas frutas que tienen las diferentes réplicas de Sum. Va sumando ellas para unificar la cantidad real pedida por un cliente sobre una fruta específica. Para saber cuando terminó de recibir información sobre una fruta para un cliente específico, contabiliza cuantos EOF recibe sobre dicho cliente hasta alcanzar un tope (en este caso, SUM_AMOUNT) para saber cuando recibió la totalidad de la cantidad pedida, luego busca las N mas grandes para enviarselas a Join.

### Join
Join sigue la misma lógica, esperando a que N replicas de Aggregator (en este caso N = AGGREGATION_AMOUNT), le envíen información sobre un cliente, y de esa manera envía al gateway de salida del sistema las N frutas de mayor pedido.

## Escalabilidad del Sistema

### Gateway
El sistema tiene un gateway de entrada, que el día de mañana podría representar un problema ya que no se replica, pero cuenta con Multiproccesing por lo que parece acompañar la escalabilidad del sistema. 

### Sum y Aggregator
En cuanto a lo trabajado, con la implementación y la coordinación entre sistemas explicados previamente en este mismo informe, sobre todo el "balanceo de carga" que podría entenderse como que las replicas de Sum envían, según el nombre de la fruta, a diferentes instancias de Aggregator, lo que le da una distribución mejor a la hora de procesar esta información; el sistema permite procesar muchos requests mas que hacer todo con instancias únicas de estos nodos y de forma serializada.

### Clientes
Los clientes se identifican por client_id y luego de procesar la solicitud en su totalidad se libera la memoria asociada a ese cliente, asique atender a nuevos request por parte de clientes se hace de una manera más escalable. 

### Volumen de Datos
La input_queue que comparten las replicas de Sum funciona con la idea de "competing consumers", asi van distribuyendo los diferentes mensajes que se deben procesar entre las instancias, favoreciendo la escalabilidad del sistema.

### Controles
Para controlar la comunicación entre replicas o entre diferentes sistemas (Sum > Aggregation o Aggregation > Join) se utilizan las variables de entorno SUM_AMOUNT y AGGREGATION_AMOUNT que se setean en el docker-compose, haciendo que se adapte la funcionalidad del sistema en su totalidad según estos parametros configurables.
