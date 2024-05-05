import ray

# Conectar al cluster de Ray (proporciona la dirección del nodo maestro)
ray.init(address='192.168.1.29:6379')  # Reemplaza '<direccion_del_maestro>' con la dirección del nodo maestro