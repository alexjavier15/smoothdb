#include "json_builder.h"

jbuilder_t *jbuilder_init(char *str)
{
    /* add deser logic here */
    jbuilder_t *builder = calloc(1, sizeof(jbuilder_t));

    if (str) {
        builder->root = json_tokener_parse(str);     
    } else {
        builder->root = json_object_new_object();
    }

    return builder;
}

void jbuilder_cleanup(jbuilder_t *builder)
{
    // freeing of root automatically frees rest, keeps valgrind happy
    json_object_put(builder->root);

    free(builder);

}

container_t *jbuilder_create_container(jbuilder_t *builder, char *cname)
{
    json_object *array = json_object_new_array();
    json_object_object_add(builder->root, cname, array);

    return array;
}

void jbuilder_add_object(jbuilder_t *builder, container_t *container, 
        char *obj_name)
{
    json_object *string = json_object_new_string(obj_name);
    json_object_array_add(container, string);
}

const char *jbuilder_serialize(jbuilder_t *builder)
{
    return json_object_to_json_string(builder->root);
}
