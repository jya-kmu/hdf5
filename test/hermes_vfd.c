#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <mpi.h>
#include <assert.h>
#include "hdf5.h"

#define DATASETNAME "IntArray"
#define NX     256                      /* dataset dimensions */
#define NY     256

int main(int argc, char *argv[]) {
    hid_t       file_id, fapl_id;
    hid_t       dset_id, dcpl_id, dataspace_id;
    hsize_t     dims[2];
    char       *file_name = "hermes_test.h5";
    int         data_in[NX][NY];          /* data to write */
    int         data_out[NX][NY];         /* data to read */
    int         i, j, k;

    int mpi_threads_provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &mpi_threads_provided);
    if (mpi_threads_provided < MPI_THREAD_MULTIPLE) {
      fprintf(stderr, "Didn't receive appropriate MPI threading specification\n");
      return 1;
    }

    printf("Calling H5Pcreate()\n");
    if((fapl_id = H5Pcreate(H5P_FILE_ACCESS)) < 0)
        printf("H5Pcreate() error\n");

    printf("Calling H5Pset_fapl_hermes()\n");
    if (H5Pset_fapl_hermes(fapl_id, false, 1024) < 0)
        printf("H5Pset_fapl_hermes() error\n");

    printf("Calling H5Fcreate()\n");
    if ((file_id = H5Fcreate(file_name, H5F_ACC_TRUNC, H5P_DEFAULT, fapl_id)) < 0)
        printf("H5Fcreate() error\n");

    dims[0] = NX;
    dims[1] = NY;
    printf("Calling H5Screate_simple()\n");
    dataspace_id = H5Screate_simple (2, dims, NULL);

    printf("Calling H5Pcreate()\n");
    dcpl_id =  H5Pcreate(H5P_DATASET_CREATE);

    printf("Calling H5Dcreate()\n");
    dset_id = H5Dcreate(file_id, DATASETNAME, H5T_NATIVE_INT, dataspace_id,
                        H5P_DEFAULT, dcpl_id, H5P_DEFAULT);

    for (j = 0; j < NX; j++) {
        for (i = 0; i < NY; i++) {
            data_in[j][i] = i + j;
            data_out[j][i] = 0;
        }
    }

    printf("Calling H5Dwrite()\n");
    if (H5Dwrite(dset_id, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT, data_in) < 0)
      printf("H5Dwrite() error\n");

    printf("Calling H5Dread()\n");
    if (H5Dread(dset_id, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT, data_out) < 0)
        printf("H5Dread() error\n");

    for (j = 0; j < NX; j++) {
        for (i = 0; i < NY; i++) {
            assert (data_out[j][i] == data_in[j][i]);
        }
    }

    printf("Calling H5Dclose()\n");
    H5Dclose(dset_id);

    printf("Calling H5Sclose()\n");
    H5Sclose(dataspace_id);

    printf("Calling H5Fclose()\n");
    H5Fclose(file_id);

    printf("Calling H5Pclose()\n");
    H5Pclose(fapl_id);

    return 0;
}
