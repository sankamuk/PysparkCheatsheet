from flask import Flask, request
from flask_restplus import Api, Resource, fields

flask_app = Flask(__name__)
app = Api(app = flask_app, 
          version = "1.0", 
          title = "KafkaACLAdmin", 
          description = "Manage ACL Kafka Cluster")

name_space = app.namespace("KafkaAdmin", description="Manage ACL Kafka Cluster")

acl_model = app.model( 'ACL Create Model', { "userName": fields.String(required = True, description="Principal", help="Cannot be blank")} )

@name_space.route("/<string:topicName>")
class MainClass(Resource):

    def get(self, topicName):
        try:
            return { 
                "userName": topicName,
                }

        except Exception as e:
            name_space.abort(500, e.__doc__, status = "Could not retrieve information", statusCode = "500")


    @app.expect(acl_model)
    def post(self, topicName):
        try:
            return { 
                "userName": request.json['userName'],
                "topicName": topicName
                }

        except Exception as e:
            name_space.abort(500, e.__doc__, status = "Could not retrieve information", statusCode = "500")



    def delete(self, topicName):
        try:
            return { 
                "topicName": topicName
                }

        except Exception as e:
            name_space.abort(500, e.__doc__, status = "Could not retrieve information", statusCode = "500")






'''
keytool -importkeystore -srckeystore user1.jks -destkeystore user1.p12 -deststoretype PKCS12
openssl pkcs12 -in user1.p12  -nodes -nocerts -out user1.key

>>> acl_filter = ACLFilter(
...     principal=None,
...     host="*",
...     operation=ACLOperation.ANY,
...     permission_type=ACLPermissionType.ANY,
...     resource_pattern=ResourcePattern(ResourceType.TOPIC, "tst")
... )
>>> admin.describe_acls(acl_filter)
([<ACL principal=User:CN=user1,OU=IT,O=SANMUK,L=PARIS,ST=ILF,C=FR, resource=<ResourcePattern type=TOPIC, name=tst, pattern=LITERAL>, operation=DESCRIBE, type=ALLOW, host=*>, <ACL principal=User:CN=user1,OU=IT,O=SANMUK,L=PARIS,ST=ILF,C=FR, resource=<ResourcePattern type=TOPIC, name=tst, pattern=LITERAL>, operation=WRITE, type=ALLOW, host=*>, <ACL principal=User:CN=user1,OU=IT,O=SANMUK,L=PARIS,ST=ILF,C=FR, resource=<ResourcePattern type=TOPIC, name=tst, pattern=LITERAL>, operation=READ, type=ALLOW, host=*>], <class 'kafka.errors.NoError'>)


>>> acl1 = ACL(
...         principal="User:CN=user1,OU=IT,O=SANMUK,L=PARIS,ST=ILF,C=FR",
...         host="*",
...         operation=ACLOperation.DESCRIBE,
...         permission_type=ACLPermissionType.ALLOW,
...         resource_pattern=ResourcePattern(ResourceType.TOPIC, "tst")
... )
>>> acl2 = ACL(
...         principal="User:CN=user1,OU=IT,O=SANMUK,L=PARIS,ST=ILF,C=FR",
...         host="*",
...         operation=ACLOperation.READ,
...         permission_type=ACLPermissionType.ALLOW,
...         resource_pattern=ResourcePattern(ResourceType.TOPIC, "tst")
... )
>>> acl3 = ACL(
...         principal="User:CN=user1,OU=IT,O=SANMUK,L=PARIS,ST=ILF,C=FR",
...         host="*",
...         operation=ACLOperation.READ,
...         permission_type=ACLPermissionType.ALLOW,
...         resource_pattern=ResourcePattern(ResourceType.GROUP, "*")
... )
>>> result = admin.create_acls([acl1, acl2, acl3])
>>> result["failed"]
[]
>>> result["succeeded"]
[<ACL principal=User:CN=user1,OU=IT,O=SANMUK,L=PARIS,ST=ILF,C=FR, resource=<ResourcePattern type=TOPIC, name=tst, pattern=LITERAL>, operation=DESCRIBE, type=ALLOW, host=*>, <ACL principal=User:CN=user1,OU=IT,O=SANMUK,L=PARIS,ST=ILF,C=FR, resource=<ResourcePattern type=TOPIC, name=tst, pattern=LITERAL>, operation=READ, type=ALLOW, host=*>, <ACL principal=User:CN=user1,OU=IT,O=SANMUK,L=PARIS,ST=ILF,C=FR, resource=<ResourcePattern type=GROUP, name=*, pattern=LITERAL>, operation=READ, type=ALLOW, host=*>]
>>>

acl1 = ACL(
        principal="User:CN=user1,OU=IT,O=SANMUK,L=PARIS,ST=ILF,C=FR",
        host="*",
        operation=ACLOperation.DESCRIBE,
        permission_type=ACLPermissionType.ALLOW,
        resource_pattern=ResourcePattern(ResourceType.TOPIC, "tst")
)
acl2 = ACL(
        principal="User:CN=user1,OU=IT,O=SANMUK,L=PARIS,ST=ILF,C=FR",
        host="*",
        operation=ACLOperation.WRITE,
        permission_type=ACLPermissionType.ALLOW,
        resource_pattern=ResourcePattern(ResourceType.TOPIC, "tst")
)
acl3 = ACL(
        principal="User:CN=user1,OU=IT,O=SANMUK,L=PARIS,ST=ILF,C=FR",
        host="*",
        operation=ACLOperation.CREATE,
        permission_type=ACLPermissionType.ALLOW,
        resource_pattern=ResourcePattern(ResourceType.CLUSTER, "kafka-cluster")
)
result = admin.create_acls([acl1, acl2, acl3])


>>> result["failed"]
[]
>>> result["succeeded"]
[<ACL principal=User:CN=user1,OU=IT,O=SANMUK,L=PARIS,ST=ILF,C=FR, resource=<ResourcePattern type=TOPIC, name=tst, pattern=LITERAL>, operation=DESCRIBE, type=ALLOW, host=*>, <ACL principal=User:CN=user1,OU=IT,O=SANMUK,L=PARIS,ST=ILF,C=FR, resource=<ResourcePattern type=TOPIC, name=tst, pattern=LITERAL>, operation=WRITE, type=ALLOW, host=*>, <ACL principal=User:CN=user1,OU=IT,O=SANMUK,L=PARIS,ST=ILF,C=FR, resource=<ResourcePattern type=CLUSTER, name=kafka-cluster, pattern=LITERAL>, operation=CREATE, type=ALLOW, host=*>]



from kafka.admin import KafkaAdminClient, ACLFilter, ACLPermissionType, ResourcePattern, ResourceType, ACL, ACLOperation

admin = KafkaAdminClient(bootstrap_servers='server:9093', security_protocol='SSL', ssl_cafile='/root/ssl/ca.cert', ssl_certfile='/data/kafka/conf/server.cert', ssl_keyfile='/data/kafka/conf/server.key')

'''
