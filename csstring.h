#ifndef __CSSTRING_H_INCLUDED__
#define __CSSTRING_H_INCLUDED__

#ifdef __cplusplus
extern "C" {
#endif


bool
csstring_is (void *self);

const char const *
csstring_data (void *self);

csstring_t*
csstring_new (const char * const data);

void
csstring_destroy (csstring_t **self_p);


#ifdef __cplusplus
}
#endif

#endif

