/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright by The HDF Group.                                               *
 * Copyright by the Board of Trustees of the University of Illinois.         *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of HDF5.  The full HDF5 copyright notice, including     *
 * terms governing use, modification, and redistribution, is contained in    *
 * the COPYING file, which can be found at the root of the source code       *
 * distribution tree, or in https://www.hdfgroup.org/licenses.               *
 * If you do not have access to either file, you may request a copy from     *
 * help@hdfgroup.org.                                                        *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/*
 * Programmer:  Kimmy Mu
 *              March 2021
 *
 * Purpose: The hermes file driver using only the HDF5 public API
 *          and buffer datasets in Hermes buffering systems with
 *          multiple storage tiers.
 */

#include "H5FDdrvr_module.h" /* This source code file is part of the H5FD driver module */

#include "H5private.h"   /* Generic Functions        */
#include "H5Eprivate.h"  /* Error handling           */
#include "H5Fprivate.h"  /* File access              */
#include "H5FDprivate.h" /* File drivers             */
#include "H5FDhermes.h"  /* Hermes file driver       */
#include "H5FLprivate.h" /* Free Lists               */
#include "H5Iprivate.h"  /* IDs                      */
#include "H5MMprivate.h" /* Memory management        */
#include "H5Pprivate.h"  /* Property lists           */


#ifdef H5_HAVE_HERMES_VFD

/* Necessary hermes headers */
#include "hermes_wrapper.h"

/* The driver identification number, initialized at runtime */
static hid_t H5FD_HERMES_g = 0;

/* Whether Hermes is initialized */
static htri_t hermes_initialized = FAIL;

/**
 * Define kPageSize mapping.
 */
const size_t kPageSize = 4 * 1024;

unsigned char buffer_page_g[kPageSize];

/**
 * Define length of blob name, which is converted from page index
 */
const size_t len_blob_name = 10;

/**
 * kHermesConf env variable is used to define path to kHermesConf in adapters.
 * This is used for initialization of Hermes.
 */
const char *kHermesConf = "HERMES_CONF";

/* The description of a file/bucket belonging to this driver. */
typedef struct H5FD_hermes_t {
    H5FD_t         pub; /* public stuff, must be first      */
    haddr_t        eoa; /* end of allocated region          */
    haddr_t        eof; /* end of file; current file size   */
    haddr_t        pos; /* current file I/O position        */
    H5FD_file_op_t op;  /* last operation                   */
    char           bktname[H5FD_MAX_FILENAME_LEN]; /* Copy of file name from open operation */
    int            o_flags; /* Flags for open() call    */
    BucketClass   *bkt_handle;
    unsigned char *page_buf;
} H5FD_hermes_t;

/*
 * These macros check for overflow of various quantities.  These macros
 * assume that HDoff_t is signed and haddr_t and size_t are unsigned.
 *
 * ADDR_OVERFLOW:   Checks whether a file address of type `haddr_t'
 *                  is too large to be represented by the second argument
 *                  of the file seek function.
 *
 * SIZE_OVERFLOW:   Checks whether a buffer size of type `hsize_t' is too
 *                  large to be represented by the `size_t' type.
 *
 * REGION_OVERFLOW: Checks whether an address and size pair describe data
 *                  which can be addressed entirely by the second
 *                  argument of the file seek function.
 */
#define MAXADDR          (((haddr_t)1 << (8 * sizeof(HDoff_t) - 1)) - 1)
#define ADDR_OVERFLOW(A) (HADDR_UNDEF == (A) || ((A) & ~(haddr_t)MAXADDR))
#define SIZE_OVERFLOW(Z) ((Z) & ~(hsize_t)MAXADDR)
#define REGION_OVERFLOW(A, Z)                                                                                \
    (ADDR_OVERFLOW(A) || SIZE_OVERFLOW(Z) || HADDR_UNDEF == (A) + (Z) || (HDoff_t)((A) + (Z)) < (HDoff_t)(A))

/* Prototypes */
static herr_t  H5FD__hermes_term(void);
static H5FD_t *H5FD__hermes_open(const char *name, unsigned flags, hid_t fapl_id, haddr_t maxaddr);
static herr_t  H5FD__hermes_close(H5FD_t *_file);
static int     H5FD__hermes_cmp(const H5FD_t *_f1, const H5FD_t *_f2);
static herr_t  H5FD__hermes_query(const H5FD_t *_f1, unsigned long *flags);
static haddr_t H5FD__hermes_get_eoa(const H5FD_t *_file, H5FD_mem_t type);
static herr_t  H5FD__hermes_set_eoa(H5FD_t *_file, H5FD_mem_t type, haddr_t addr);
static haddr_t H5FD__hermes_get_eof(const H5FD_t *_file, H5FD_mem_t type);
static herr_t  H5FD__hermes_read(H5FD_t *_file, H5FD_mem_t type, hid_t fapl_id, haddr_t addr, size_t size,
                               void *buf);
static herr_t  H5FD__hermes_write(H5FD_t *_file, H5FD_mem_t type, hid_t fapl_id, haddr_t addr, size_t size,
                                const void *buf);

static const H5FD_class_t H5FD_hermes_g = {
    "hermes",                  /* name                 */
    MAXADDR,                   /* maxaddr              */
    H5F_CLOSE_STRONG,          /* fc_degree            */
    H5FD__hermes_term,         /* terminate            */
    NULL,                      /* sb_size              */
    NULL,                      /* sb_encode            */
    NULL,                      /* sb_decode            */
    0,                         /* fapl_size            */
    NULL,                      /* fapl_get             */
    NULL,                      /* fapl_copy            */
    NULL,                      /* fapl_free            */
    0,                         /* dxpl_size            */
    NULL,                      /* dxpl_copy            */
    NULL,                      /* dxpl_free            */
    H5FD__hermes_open,         /* open                 */
    H5FD__hermes_close,        /* close                */
    H5FD__hermes_cmp,          /* cmp                  */
    H5FD__hermes_query,        /* query                */
    NULL,                      /* get_type_map         */
    NULL,                      /* alloc                */
    NULL,                      /* free                 */
    H5FD__hermes_get_eoa,      /* get_eoa              */
    H5FD__hermes_set_eoa,      /* set_eoa              */
    H5FD__hermes_get_eof,      /* get_eof              */
    NULL,                      /* get_handle           */
    H5FD__hermes_read,         /* read                 */
    H5FD__hermes_write,        /* write                */
    NULL,                      /* flush                */
    NULL,                      /* truncate             */
    NULL,                      /* lock                 */
    NULL,                      /* unlock               */
    H5FD_FLMAP_DICHOTOMY       /* fl_map               */
};

/* Declare a free list to manage the H5FD_hermes_t struct */
H5FL_DEFINE_STATIC(H5FD_hermes_t);

/*-------------------------------------------------------------------------
 * Function:    H5FD__init_package
 *
 * Purpose:     Initializes any interface-specific data or routines.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__init_package(void)
{
    herr_t ret_value    = SUCCEED;

    FUNC_ENTER_STATIC

    if (H5FD_hermes_init() < 0)
        HGOTO_ERROR(H5E_VFL, H5E_CANTINIT, FAIL, "unable to initialize hermes VFD")

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__init_package() */

/*-------------------------------------------------------------------------
 * Function:    H5FD_hermes_init
 *
 * Purpose:     Initialize this driver by registering the driver with the
 *              library.
 *
 * Return:      Success:    The driver ID for the hermes driver
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5FD_hermes_init(void)
{
    hid_t ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_NOAPI(H5I_INVALID_HID)

    if (H5I_VFL != H5I_get_type(H5FD_HERMES_g))
        H5FD_HERMES_g = H5FD_register(&H5FD_hermes_g, sizeof(H5FD_class_t), FALSE);

    /* Set return value */
    ret_value = H5FD_HERMES_g;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD_hermes_init() */

/*---------------------------------------------------------------------------
 * Function:    H5FD__hermes_term
 *
 * Purpose:     Shut down the VFD
 *
 * Returns:     SUCCEED (Can't fail)
 *
 *---------------------------------------------------------------------------
 */
static herr_t
H5FD__hermes_term(void)
{
    FUNC_ENTER_STATIC_NOERR

    /* Reset VFL ID */
    H5FD_HERMES_g = 0;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5FD__hermes_term() */

/*-------------------------------------------------------------------------
 * Function:    H5Pset_fapl_hermes
 *
 * Purpose:     Modify the file access property list to use the H5FD_HERMES
 *              driver defined in this source file.  There are no driver
 *              specific properties.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_fapl_hermes(hid_t fapl_id)
{
    H5P_genplist_t *plist; /* Property list pointer */
    herr_t          ret_value;

    FUNC_ENTER_API(FAIL)
    H5TRACE1("e", "i", fapl_id);

    if (NULL == (plist = H5P_object_verify(fapl_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file access property list")

    ret_value = H5P_set_driver(plist, H5FD_HERMES, NULL);

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_fapl_hermes() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__hermes_open
 *
 * Purpose:     Create and/or opens a bucket in Hermes.
 *
 * Return:      Success:    A pointer to a new bucket data structure.
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static H5FD_t *
H5FD__hermes_open(const char *name, unsigned flags, hid_t fapl_id, haddr_t maxaddr)
{
    H5FD_hermes_t  *file = NULL; /* hermes VFD info            */
    int             o_flags;     /* Flags for open() call    */
    H5P_genplist_t *plist;            /* Property list pointer */
    char           *hermes_config = NULL;
    H5FD_t         *ret_value = NULL; /* Return value */

    FUNC_ENTER_STATIC

    /* Sanity check on file offsets */
    HDcompile_assert(sizeof(HDoff_t) >= sizeof(size_t));

    /* Check arguments */
    if (!name || !*name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "invalid file name")
    if (0 == maxaddr || HADDR_UNDEF == maxaddr)
        HGOTO_ERROR(H5E_ARGS, H5E_BADRANGE, NULL, "bogus maxaddr")
    if (ADDR_OVERFLOW(maxaddr))
        HGOTO_ERROR(H5E_ARGS, H5E_OVERFLOW, NULL, "bogus maxaddr")

    /* Build the open flags */
    o_flags = (H5F_ACC_RDWR & flags) ? O_RDWR : O_RDONLY;
    if (H5F_ACC_TRUNC & flags)
        o_flags |= O_TRUNC;
    if (H5F_ACC_CREAT & flags)
        o_flags |= O_CREAT;
    if (H5F_ACC_EXCL & flags)
        o_flags |= O_EXCL;

    /* Initialize Hermes */
    if ((H5OPEN hermes_initialized) == FAIL) {
        hermes_config = HDgetenv(kHermesConf);
        if (HermesInitHermes(hermes_config) < 0) {
            HGOTO_ERROR(H5E_SYM, H5E_UNINITIALIZED, NULL, "Hermes initialization failed")
        }
        else {
            hermes_initialized = TRUE;
        }
    }

    /* Create the new file struct */
    if (NULL == (file = H5FL_CALLOC(H5FD_hermes_t)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "unable to allocate file struct")

    /* Get the FAPL */
    if (NULL == (plist = (H5P_genplist_t *)H5I_object(fapl_id)))
        HGOTO_ERROR(H5E_VFL, H5E_BADTYPE, NULL, "not a file access property list")

    /* Retain a copy of the name used to open the file, for possible error reporting */
    HDstrncpy(file->bktname, name, sizeof(file->bktname));
    file->bktname[sizeof(file->bktname) - 1] = '\0';

    file->bkt_handle = HermesBucketCreate(name);

    /* Hardcoded now */
    file->page_buf = &buffer_page_g[0];

    /* Set return value */
    ret_value = (H5FD_t *)file;

done:
    if (NULL == ret_value) {
        if (file)
            file = H5FL_FREE(H5FD_hermes_t, file);
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__hermes_open() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__hermes_close
 *
 * Purpose:     Closes an HDF5 file.
 *
 * Return:      Success:    SUCCEED
 *              Failure:    FAIL, file not closed.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__hermes_close(H5FD_t *_file)
{
    H5FD_hermes_t *file = (H5FD_hermes_t *)_file;
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_STATIC_NOERR

    /* Sanity check */
    HDassert(file);

    HermesBucketClose(file->bkt_handle);

    /* Release the file info */
    file = H5FL_FREE(H5FD_hermes_t, file);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__hermes_close() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__hermes_cmp
 *
 * Purpose:     Compares two buckets belonging to this driver using an
 *              arbitrary (but consistent) ordering.
 *
 * Return:      Success:    A value like strcmp()
 *              Failure:    never fails (arguments were checked by the
 *                          caller).
 *
 *-------------------------------------------------------------------------
 */
static int
H5FD__hermes_cmp(const H5FD_t *_f1, const H5FD_t *_f2)
{
    const H5FD_hermes_t *f1        = (const H5FD_hermes_t *)_f1;
    const H5FD_hermes_t *f2        = (const H5FD_hermes_t *)_f2;
    int                  ret_value = 0;

    FUNC_ENTER_STATIC_NOERR

    ret_value = strcmp(f1->bktname, f2->bktname);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__hermes_cmp() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__hermes_query
 *
 * Purpose:     Set the flags that this VFL driver is capable of supporting.
 *              (listed in H5FDpublic.h)
 *
 * Return:      SUCCEED (Can't fail)
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__hermes_query(const H5FD_t *_file, unsigned long *flags /* out */)
{
    FUNC_ENTER_STATIC_NOERR

    /* Set the VFL feature flags that this driver supports */
    /* Notice: the Mirror VFD Writer currently uses only the hermes driver as
     * the underying driver -- as such, the Mirror VFD implementation copies
     * these feature flags as its own. Any modifications made here must be
     * reflected in H5FDmirror.c
     * -- JOS 2020-01-13
     */
    if (flags) {
        *flags = 0;
    }                                            /* end if */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5FD__hermes_query() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__hermes_get_eoa
 *
 * Purpose:     Gets the end-of-address marker for the file. The EOA marker
 *              is the first address past the last byte allocated in the
 *              format address space.
 *
 * Return:      The end-of-address marker.
 *
 *-------------------------------------------------------------------------
 */
static haddr_t
H5FD__hermes_get_eoa(const H5FD_t *_file, H5FD_mem_t H5_ATTR_UNUSED type)
{
    const H5FD_hermes_t *file = (const H5FD_hermes_t *)_file;

    FUNC_ENTER_STATIC_NOERR

    FUNC_LEAVE_NOAPI(file->eoa)
} /* end H5FD__hermes_get_eoa() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__hermes_set_eoa
 *
 * Purpose:     Set the end-of-address marker for the file. This function is
 *              called shortly after an existing HDF5 file is opened in order
 *              to tell the driver where the end of the HDF5 data is located.
 *
 * Return:      SUCCEED (Can't fail)
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__hermes_set_eoa(H5FD_t *_file, H5FD_mem_t H5_ATTR_UNUSED type, haddr_t addr)
{
    H5FD_hermes_t *file = (H5FD_hermes_t *)_file;

    FUNC_ENTER_STATIC_NOERR

    file->eoa = addr;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5FD__hermes_set_eoa() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__hermes_get_eof
 *
 * Purpose:     Returns the end-of-file marker, which is the greater of
 *              either the filesystem end-of-file or the HDF5 end-of-address
 *              markers.
 *
 * Return:      End of file address, the first address past the end of the
 *              "file", either the filesystem file or the HDF5 file.
 *
 *-------------------------------------------------------------------------
 */
static haddr_t
H5FD__hermes_get_eof(const H5FD_t *_file, H5FD_mem_t H5_ATTR_UNUSED type)
{
    const H5FD_hermes_t *file = (const H5FD_hermes_t *)_file;

    FUNC_ENTER_STATIC_NOERR

    FUNC_LEAVE_NOAPI(file->eof)
} /* end H5FD__hermes_get_eof() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__hermes_read
 *
 * Purpose:     Reads SIZE bytes of data from FILE beginning at address ADDR
 *              into buffer BUF according to data transfer properties in
 *              DXPL_ID. Determine the number of file pages affected by this
 *              call from ADDR and SIZE. Utilize transfer buffer PAGE_BUF to
 *              read the data from Blobs. Exercise care for the first and last
 *              pages to prevent overwriting existing data.
 *
 * Return:      Success:    SUCCEED. Result is stored in caller-supplied
 *                          buffer BUF.
 *              Failure:    FAIL, Contents of buffer BUF are undefined.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__hermes_read(H5FD_t *_file, H5FD_mem_t H5_ATTR_UNUSED type, hid_t H5_ATTR_UNUSED dxpl_id, haddr_t addr,
                size_t size, void *buf /*out*/)
{
    H5FD_hermes_t *file      = (H5FD_hermes_t *)_file;
    size_t         num_pages; /* Number of pages of transfer buffer */
    size_t         start_page_index; /* First page index of tranfer buffer */
    size_t         end_page_index; /* End page index of tranfer buffer */
    size_t         transfer_size = 0;
    size_t         k;
    haddr_t        addr_end = addr+size-1;
    herr_t         ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_STATIC

    HDassert(file && file->pub.cls);
    HDassert(buf);

    /* Check for overflow conditions */
    if (!H5F_addr_defined(addr))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "addr undefined, addr = %llu", (unsigned long long)addr)
    if (REGION_OVERFLOW(addr, size))
        HGOTO_ERROR(H5E_ARGS, H5E_OVERFLOW, FAIL, "addr overflow, addr = %llu", (unsigned long long)addr)
    if (NULL == file->page_buf)
        HGOTO_ERROR(H5E_INTERNAL, H5E_UNINITIALIZED, FAIL, "transfer buffer not initialized")

    start_page_index = addr/kPageSize;
    end_page_index = (addr+size)/kPageSize;
    num_pages = end_page_index - start_page_index + 1;

    for (k=start_page_index; k<=end_page_index; ++k) {
        char k_blob[len_blob_name];
        snprintf(k_blob, 6, "%zu\n", k);
        /* Check if this blob exists */
        bool blob_exists = HermesBucketContainsBlob(file->bkt_handle, k_blob);
        /* Read blob back to transfer buffer */
        if (!blob_exists)
            HGOTO_ERROR(H5E_INTERNAL, H5E_BADVALUE, FAIL, "not able to retrieve Blob %zu", k)
        /* Read blob back to transfer buffer */
        HermesBucketGet(file->bkt_handle, k_blob, kPageSize, file->page_buf);

        /* Check if addr is in the range of (k*kPageSize, (k+1)*kPageSize) */
        /* NOTE: The range does NOT include the start address of page k,
           but includes the end address of page k */
        if (addr > k*kPageSize && addr < (k+1)*kPageSize) {
            /* Calculate the starting address of transfer buffer update within page k */
            size_t offset = addr - k*kPageSize;
            HDassert(offset > 0);

            /* Copy update to BUF */
            /* addr+size is within the same page (only one page) */
            if (addr_end <= (k+1)*kPageSize-1) {
                memcpy(buf, file->page_buf+offset, size);
                transfer_size += size;
            }
            /* More than one page */
            else {
                /* Copy data from addr to the end of the address in page k */
                memcpy(buf, file->page_buf+offset, (k+1)*kPageSize-addr);
                transfer_size += (k+1)*kPageSize-addr;
            }
        }
        /* Check if addr_end is in the range of [k*kPageSize, (k+1)*kPageSize-1) */
        /* NOTE: The range includes the start address of page k,
           but does NOT include the end address of page k */
        else if (addr_end >= k*kPageSize && addr_end < (k+1)*kPageSize-1) {
            /* Update transfer buffer */
            memcpy(buf+transfer_size, file->page_buf, addr_end-k*kPageSize+1);
            transfer_size += addr_end-k*kPageSize+1;
        }
        /* Page/Blob k is within the range of (addr, addr+size) */
        else if (addr <= k*kPageSize && addr_end >= (k+1)*kPageSize-1) {
            /* Update transfer buffer */
            memcpy(buf+transfer_size, file->page_buf, kPageSize);
            transfer_size += kPageSize;
        }
    }

    /* Update current position */
    file->pos = addr+size;
    file->op  = OP_READ;

done:
    if (ret_value < 0) {
        /* Reset last file I/O information */
        file->pos = HADDR_UNDEF;
        file->op  = OP_UNKNOWN;
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__hermes_read() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__hermes_write
 *
 * Purpose:     Writes SIZE bytes of data contained in buffer BUF to Hermes
 *              buffering system according to data transfer properties in
 *              DXPL_ID. Determine the number of file pages affected by this
 *              call from ADDR and SIZE. Utilize transfer buffer PAGE_BUF to
 *              put the data into Blobs. Exercise care for the first and last
 *              pages to prevent overwriting existing data.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__hermes_write(H5FD_t *_file, H5FD_mem_t H5_ATTR_UNUSED type, hid_t H5_ATTR_UNUSED dxpl_id, haddr_t addr,
                 size_t size, const void *buf)
{
    H5FD_hermes_t *file      = (H5FD_hermes_t *)_file;
    size_t         num_pages; /* Number of pages of transfer buffer */
    size_t         start_page_index; /* First page index of tranfer buffer */
    size_t         end_page_index; /* End page index of tranfer buffer */
    size_t         transfer_size = 0;
    size_t         k;
    haddr_t        addr_end = addr+size-1;
    herr_t         ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_STATIC

    HDassert(file && file->pub.cls);
    HDassert(buf);

    /* Check for overflow conditions */
    if (!H5F_addr_defined(addr))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "addr undefined, addr = %llu", (unsigned long long)addr)
    if (REGION_OVERFLOW(addr, size))
        HGOTO_ERROR(H5E_ARGS, H5E_OVERFLOW, FAIL, "addr overflow, addr = %llu, size = %llu",
                    (unsigned long long)addr, (unsigned long long)size)
    if (NULL == file->page_buf)
        HGOTO_ERROR(H5E_INTERNAL, H5E_UNINITIALIZED, FAIL, "transfer buffer not initialized")

    start_page_index = addr/kPageSize;
    end_page_index = (addr+size)/kPageSize;
    num_pages = end_page_index - start_page_index + 1;

    /* Assume only using one page now */
    for (k=start_page_index; k<=end_page_index; ++k) {
        char k_blob[50];
        snprintf(k_blob, 6, "%zu\n", k);
        /* Check if addr is in the range of (k*kPageSize, (k+1)*kPageSize) */
        /* NOTE: The range does NOT include the start address of page k,
           but includes the end address of page k */
        if (addr > k*kPageSize && addr < (k+1)*kPageSize) {
            /* Check if this blob exists */
            bool blob_exists = HermesBucketContainsBlob(file->bkt_handle, k_blob);
            /* Read blob back to transfer buffer */
            if (blob_exists) {
                HermesBucketGet(file->bkt_handle, k_blob, kPageSize, file->page_buf);
            }
            /* Calculate the starting address of transfer buffer update within page k */
            size_t offset = addr - k*kPageSize;
            HDassert(offset > 0);

            /* Update transfer buffer */
            /* addr+size is within the same page (only one page) */
            if (addr_end <= (k+1)*kPageSize-1) {
                memcpy(file->page_buf+offset, buf+transfer_size, size);
                transfer_size += size;
            }
            /* More than one page */
            else {
                /* Copy data from addr to the end of the address in page k */
                memcpy(file->page_buf+offset, buf+transfer_size, (k+1)*kPageSize-addr);
                transfer_size += (k+1)*kPageSize-addr;
            }
        }
        /* Check if addr_end is in the range of [k*kPageSize, (k+1)*kPageSize-1) */
        /* NOTE: The range includes the start address of page k,
           but does NOT include the end address of page k */
        else if (addr_end >= k*kPageSize && addr_end < (k+1)*kPageSize-1) {
            /* Check if this blob exists */
            bool blob_exists = HermesBucketContainsBlob(file->bkt_handle, k_blob);
            /* Read blob back */
            if (blob_exists) {
                unsigned char *put_data_ptr = (unsigned char *)file->page_buf;
                HermesBucketGet(file->bkt_handle, k_blob, kPageSize, file->page_buf);
            }
            /* Update transfer buffer */
            memcpy(file->page_buf, buf+transfer_size, addr_end-k*kPageSize+1);
            transfer_size += addr_end-k*kPageSize+1;
        }
        /* Page/Blob k is within the range of (addr, addr+size) */
        else if (addr <= k*kPageSize && addr_end >= (k+1)*kPageSize-1) {
            /* Update transfer buffer */
            memcpy(file->page_buf, buf+transfer_size, kPageSize);
            transfer_size += kPageSize;
        }
        /* Write Blob k to Hermes buffering system */
        HermesBucketPut(file->bkt_handle, k_blob, file->page_buf, kPageSize);
    }

    /* Update current position and eof */
    file->pos = addr+size;
    file->op  = OP_WRITE;
    if (file->pos > file->eof)
        file->eof = file->pos;

done:
    if (ret_value < 0) {
        /* Reset last file I/O information */
        file->pos = HADDR_UNDEF;
        file->op  = OP_UNKNOWN;
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__hermes_write() */

#endif /* H5_HAVE_HERMES_VFD */
