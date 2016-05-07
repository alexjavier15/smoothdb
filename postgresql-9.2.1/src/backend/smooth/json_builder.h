#ifndef __JSON_BUILDER__
#define __JSON_BUILDER__

#include <json/json.h>
#include <stdio.h>
#include <assert.h>

typedef struct {
    json_object *root;
} jbuilder_t;

typedef json_object container_t;

jbuilder_t *jbuilder_init(char *json_string);
void jbuilder_cleanup(jbuilder_t *builder);

container_t *jbuilder_create_container(jbuilder_t *builder, char *cname);

void jbuilder_add_object(jbuilder_t *builder, container_t *container, 
        char *obj_name);

const char *jbuilder_serialize(jbuilder_t *builder);

#endif
