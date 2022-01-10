# Libraries
import psycopg2
from pyservicebinding import binding
 
# DB Binding Function 
def db_bind(args_dict):
    try:
        sb = binding.ServiceBinding()
    except binding.ServiceBindingRootMissingError as msg:
        # log the error message and retry/exit
        print("SERVICE_BINDING_ROOT env var not set")
    bindings_list = sb.bindings("postgresql", "Red Hat DBaaS / Crunchy Bridge")
    print(bindings_list)
    return {'DB Binding': 'success', 'user': bindings_list[0].get('username') , 'password': bindings_list[0].get('password') , 'database': bindings_list[0].get('database') , 'host': bindings_list[0].get('host'), 'port': bindings_list[0].get('port')}
