#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <mpi.h>
#include "hdf5.h"

#define KB            1024U
#define HERMES_INCREMENT (4 * KB)

#define FILE        "SDS.h5"
#define DATASETNAME "IntArray"
#define NX     5                      /* dataset dimensions */
#define NY     6
#define RANK   2

int main(int argc, char *argv[]) {
    hid_t file_id, fapl_id;
    hid_t dset_id, dcpl_id, dataspace_id;
    hid_t driver_id = -1;
    hsize_t     dims[2];
    char *file_name = "hermes_test";
    int           data[NX][NY];          /* data to write */
    int           i, j;
    
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
    if (H5Pset_fapl_hermes(fapl_id) < 0)
        printf("H5Pset_fapl_hermes() error\n");
        
    printf("Calling H5Pget_driver()\n");
    if ((driver_id = H5Pget_driver(fapl_id)) < 0)
        printf("H5Pget_driver() error\n");
    else
        printf("driver_id = %d\n", driver_id);

    printf("Calling H5Fcreate()\n");
    if ((file_id = H5Fcreate(file_name, H5F_ACC_TRUNC, H5P_DEFAULT, fapl_id)) < 0)
//    if ((file_id = H5Fcreate(file_name, H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT)) < 0)
        printf("H5Fcreate() error\n");
 
    for (j = 0; j < NX; j++) {
        for (i = 0; i < NY; i++)
            data[j][i] = i + j;
    }
    dims[0] = NX;
    dims[1] = NY;
    printf("Calling H5Screate_simple()\n");
    dataspace_id = H5Screate_simple (2, dims, NULL);
    
    printf("Calling H5Pcreate()\n");
    dcpl_id =  H5Pcreate(H5P_DATASET_CREATE);
    
    printf("Calling H5Dcreate()\n");
    dset_id = H5Dcreate(file_id, DATASETNAME, H5T_NATIVE_INT, dataspace_id,
                              H5P_DEFAULT, dcpl_id, H5P_DEFAULT);
    
    printf("Calling H5Dwrite()\n");
    if (H5Dwrite(dset_id, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT, data) < 0)
    printf("H5Dwrite() error\n");
    
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
