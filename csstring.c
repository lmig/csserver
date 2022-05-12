#include "cs.h"

#define CSSTRING_TAG   0x0000beef

// <Definition>

struct _csstring_t {
  uint32_t tag;
  char *data;
};

//  --------------------------------------------------------------------------
//  <Description>
//  <Returns>

bool
csstring_is (void *self)
{
  assert (self);
  return ((csstring_t *) self)->tag == CSSTRING_TAG;
}


//  --------------------------------------------------------------------------
//  <Description>
//  <Returns>

const char const *
csstring_data (void *self)
{
  assert (self);
  return ((csstring_t *) self)->data;
}


//  --------------------------------------------------------------------------
//  <Description>
//  <Returns>

csstring_t*
csstring_new (const char * const data)
{
  csstring_t *self = (csstring_t *) zmalloc (sizeof (csstring_t));
  if (self) {
    size_t len = strlen (data);
    self->tag = CSSTRING_TAG;
    self->data = (char *) zmalloc (len * sizeof (char) + sizeof (char));
    memcpy (self->data, data, len);
  }

  return self;
}


//  --------------------------------------------------------------------------
//  <Description>
//  <Returns>

void
csstring_destroy (csstring_t **self_p)
{
  assert (self_p);
  if (*self_p) {
    csstring_t *self = *self_p;
    assert (csstring_is (self));
    free (self->data);
    self->data = NULL;
    free (self);
    *self_p = NULL;
  }
}
