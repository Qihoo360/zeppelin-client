#include <Python.h>
#include "libzp/include/zp_client.h"

static void zeppelin_free(void *ptr)
{
    libzp::Client *b = static_cast<libzp::Client *>(ptr);
    delete b;
    return;
}

static PyObject *create_client(PyObject *, PyObject* args)
{
   char* table = NULL;
   char* hostname = NULL;
   int port = 9866;

   if (!PyArg_ParseTuple(args, "sis", &hostname, &port, &table)) {
       return NULL;
   }
   
   libzp::Client *b = new libzp::Client(hostname, port, table);
   if (b == NULL) {
       return NULL;
   }

   return PyCObject_FromVoidPtr(b, zeppelin_free);
}

static PyObject *remove_client(PyObject *, PyObject* args)
{
    PyObject *pyb = NULL;
    if (!PyArg_ParseTuple(args, "O", &pyb)) {
        return Py_BuildValue("(is)", -1, "ParseTuple Failed");
    }

    void *vb = PyCObject_AsVoidPtr(pyb);
    libzp::Client *b = static_cast<libzp::Client *>(vb);
    if (b) {
      delete b;
    }
    return NULL;
}

static PyObject *set(PyObject *, PyObject* args)
{
    PyObject *pyb = NULL;
    char* key = NULL;
    char* val = NULL;
    int kl=0, vl=0;

    if (!PyArg_ParseTuple(args, "Os#s#", &pyb, &key, &kl, &val, &vl)) {
        return Py_BuildValue("(is)", -1, "ParseTuple Failed");
    }

    void *vb = PyCObject_AsVoidPtr(pyb);
    libzp::Client *b = static_cast<libzp::Client *>(vb);
    libzp::Status s = b->Set(std::string(key, kl), std::string(val, vl));
    if (!s.ok()) {
        return Py_BuildValue("(is#)", -2, s.ToString().data(), s.ToString().size());
    }

    return Py_BuildValue("(is)", 0, "Set OK");
}

static PyObject *get(PyObject *, PyObject* args)
{
    PyObject *pyb = NULL;
    char* key = NULL;
    int kl=0;

    if (!PyArg_ParseTuple(args, "Os#", &pyb, &key, &kl)) {
        return Py_BuildValue("(is#)", -1, "ParseTuple Failed");
    }

    void *vb = PyCObject_AsVoidPtr(pyb);
    libzp::Client *b = static_cast<libzp::Client *>(vb);
    std::string val;
    libzp::Status s = b->Get(std::string(key, kl), &val);
    if (s.IsNotFound()) {
        return Py_BuildValue("(is)", 1, NULL);
    } else if (!s.ok()) {
        return Py_BuildValue("(is#)", -2, s.ToString().data(), s.ToString().size());
    }

    return Py_BuildValue("(is#)", 0, val.data(), val.size());
}

static PyObject *mget(PyObject *, PyObject* args)
{
    PyObject *pyb = NULL;
    PyObject *keylist = NULL;

    if (!PyArg_ParseTuple(args, "OO", &pyb, &keylist)) {
        return Py_BuildValue("(is#)", -1, "ParseTuple Failed");
    }
    if (!PyList_Check(keylist)) {
        return Py_BuildValue("(is#)", -1, "ParseTuple Failed");
    }

    std::vector<std::string> keys;
    char* key = NULL;
    int kl = 0;
    for(Py_ssize_t i = 0; i < PyList_Size(keylist); i++) {
      PyObject *k = PyList_GetItem(keylist, i);
      if (!PyArg_Parse(k, "s#", &key, &kl)) {
        return Py_BuildValue("(is#)", -1, "Parse key Failed");
      }
      keys.push_back(std::string(key, kl));
    }

    std::map<std::string, std::string> values;

    void *vb = PyCObject_AsVoidPtr(pyb);
    libzp::Client *b = static_cast<libzp::Client *>(vb);
    std::string val;
    libzp::Status s = b->Mget(keys, &values);
    if (s.IsNotFound()) {
        return Py_BuildValue("(is)", 1, NULL);
    } else if (!s.ok()) {
        return Py_BuildValue("(is#)", -2, s.ToString().data(), s.ToString().size());
    }

    PyObject* res_list = PyList_New(0);
    for (auto& kv : values) {
      if (kv.second.empty()) {
        continue;
      }
      PyObject* item = Py_BuildValue("(s#s#)",
        kv.first.data(), kv.first.size(),
        kv.second.data(), kv.second.size());
      int ret = PyList_Append(res_list, item);
      if (ret != 0) {
        return Py_BuildValue("(is)", ret, "SetItem Failed");
      }
    }

    return Py_BuildValue("(iO)", 0, res_list);
}

static PyObject *zeppelin_delete(PyObject *, PyObject* args)
{
    PyObject *pyb = NULL;
    char* key = NULL;
    int kl=0;

    if (!PyArg_ParseTuple(args, "Os#", &pyb, &key, &kl)) {
        return Py_BuildValue("(is#)", -1, "ParseTuple Failed");
    }

    void *vb = PyCObject_AsVoidPtr(pyb);
    libzp::Client *b = static_cast<libzp::Client *>(vb);
    libzp::Status s = b->Delete(std::string(key, kl));
    if (!s.ok()) {
        return Py_BuildValue("(is#)", -2, s.ToString().data(), s.ToString().size());
    }

    return Py_BuildValue("(is)", 0, "Delete OK");
}

static PyMethodDef pyzeppelin_methods[] = {
    {"create_client",       create_client,       METH_VARARGS},
    {"set",                 set,                 METH_VARARGS},
    {"get",                 get,                 METH_VARARGS},
    {"mget",                mget,                METH_VARARGS},
    {"delete",              zeppelin_delete,     METH_VARARGS},
    {"remove_client",       remove_client,       METH_VARARGS},
    {NULL, NULL}
};

PyMODINIT_FUNC initpyzeppelin (void)
{
    Py_InitModule("pyzeppelin", pyzeppelin_methods);
}

