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
 * Programmer:  Robb Matzke
 *              Thursday, July 29, 1999
 *
 * Purpose: The hermes unbuffered file driver using only the HDF5 public
 *          API and with a few optimizations: the lseek() call is made
 *          only when the current file position is unknown or needs to be
 *          changed based on previous I/O through this driver (don't mix
 *          I/O from this driver with I/O from other parts of the
 *          application to the same file).
 */

#include "H5FDdrvr_module.h" /* This source code file is part of the H5FD driver module */

#include "H5private.h"   /* Generic Functions        */
#include "H5Eprivate.h"  /* Error handling           */
#include "H5Fprivate.h"  /* File access              */
#include "H5FDprivate.h" /* File drivers             */
#include "H5FDhermes.h"    /* Hermes file driver         */
#include "H5FLprivate.h" /* Free Lists               */
#include "H5Iprivate.h"  /* IDs                      */
#include "H5MMprivate.h" /* Memory management        */
#include "H5Pprivate.h"  /* Property lists           */

/* The driver identification number, initialized at runtime */
static hid_t H5FD_HERMES_g = 0;

/* Whether Hermes is initialized */
static htri_t hermes_initialized = FAIL;

const char *kHermesConf = "HERMES_CONF";

/* The description of a file belonging to this driver. The 'eoa' and 'eof'
 * determine the amount of hdf5 address space in use and the high-water mark
 * of the file (the current size of the underlying filesystem file). The
 * 'pos' value is used to eliminate file position updates when they would be a
 * no-op. Unfortunately we've found systems that use separate file position
 * indicators for reading and writing so the lseek can only be eliminated if
 * the current operation is the same as the previous operation.  When opening
 * a file the 'eof' will be set to the current file size, `eoa' will be set
 * to zero, 'pos' will be set to H5F_ADDR_UNDEF (as it is when an error
 * occurs), and 'op' will be set to H5F_OP_UNKNOWN.
 */
typedef struct H5FD_hermes_t {
    H5FD_t         pub; /* public stuff, must be first      */
    int            fd;  /* the filesystem file descriptor   */
    haddr_t        eoa; /* end of allocated region          */
    haddr_t        eof; /* end of file; current file size   */
    haddr_t        pos; /* current file I/O position        */
    H5FD_file_op_t op;  /* last operation                   */
    char           filename[H5FD_MAX_FILENAME_LEN]; /* Copy of file name from open operation */
    int            o_flags;     /* Flags for open() call    */
    off_t          st_size;        /* total size, in bytes */
    off_t          st_ptr;         /* Current ptr of FILE */
    int32_t        ref_count;        /* # of time process opens a file */
    char *st_blobs[100];         /* Blobs access in the bucket */
    blksize_t st_blksize; /* blocksize for blob within bucket */
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
static herr_t  H5FD__hermes_get_handle(H5FD_t *_file, hid_t fapl, void **file_handle);
static herr_t  H5FD__hermes_read(H5FD_t *_file, H5FD_mem_t type, hid_t fapl_id, haddr_t addr, size_t size,
                               void *buf);
static herr_t  H5FD__hermes_write(H5FD_t *_file, H5FD_mem_t type, hid_t fapl_id, haddr_t addr, size_t size,
                                const void *buf);
static herr_t  H5FD__hermes_truncate(H5FD_t *_file, hid_t dxpl_id, hbool_t closing);

static const H5FD_class_t H5FD_hermes_g = {
    "hermes",                /* name                 */
    MAXADDR,               /* maxaddr              */
    H5F_CLOSE_WEAK,        /* fc_degree            */
    H5FD__hermes_term,       /* terminate            */
    NULL,                  /* sb_size              */
    NULL,                  /* sb_encode            */
    NULL,                  /* sb_decode            */
    0,                     /* fapl_size            */
    NULL,                  /* fapl_get             */
    NULL,                  /* fapl_copy            */
    NULL,                  /* fapl_free            */
    0,                     /* dxpl_size            */
    NULL,                  /* dxpl_copy            */
    NULL,                  /* dxpl_free            */
    H5FD__hermes_open,       /* open                 */
    H5FD__hermes_close,      /* close                */
    H5FD__hermes_cmp,        /* cmp                  */
    H5FD__hermes_query,      /* query                */
    NULL,                  /* get_type_map         */
    NULL,                  /* alloc                */
    NULL,                  /* free                 */
    H5FD__hermes_get_eoa,    /* get_eoa              */
    H5FD__hermes_set_eoa,    /* set_eoa              */
    H5FD__hermes_get_eof,    /* get_eof              */
    H5FD__hermes_get_handle, /* get_handle           */
    H5FD__hermes_read,       /* read                 */
    H5FD__hermes_write,      /* write                */
    NULL,                  /* flush                */
    H5FD__hermes_truncate,   /* truncate             */
    NULL,       /* lock                 */
    NULL,     /* unlock               */
    H5FD_FLMAP_DICHOTOMY   /* fl_map               */
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
    printf("    Enter H5FD__init_package()\n");

    if (H5FD_hermes_init() < 0)
        HGOTO_ERROR(H5E_VFL, H5E_CANTINIT, FAIL, "unable to initialize hermes VFD")

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5FD__init_package() */

/*-------------------------------------------------------------------------
 * Function:    H5FD_hermes_init
 *
 * Purpose:     Initialize this driver by registering the driver with the
 *              library.
 *
 * Return:      Success:    The driver ID for the hermes driver
 *              Failure:    H5I_INVALID_HID
 *
 * Programmer:  Robb Matzke
 *              Thursday, July 29, 1999
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5FD_hermes_init(void)
{
    hid_t ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_NOAPI(H5I_INVALID_HID)
    printf("    Enter H5FD_hermes_init(), H5FD_HERMES_g = %d\n", H5FD_HERMES_g);

    if (H5I_VFL != H5I_get_type(H5FD_HERMES_g))
        H5FD_HERMES_g = H5FD_register(&H5FD_hermes_g, sizeof(H5FD_class_t), FALSE);

    printf("    End of H5FD_hermes_init(), H5FD_HERMES_g = %d\n", H5FD_HERMES_g);
    
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
 * Programmer:  Quincey Koziol
 *              Friday, Jan 30, 2004
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
 * Programmer:  Robb Matzke
 *              Thursday, February 19, 1998
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

    printf("    Call H5P_set_driver()\n");
    ret_value = H5P_set_driver(plist, H5FD_HERMES, NULL);

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_fapl_hermes() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__hermes_open
 *
 * Purpose:     Create and/or opens a file as an HDF5 file.
 *
 * Return:      Success:    A pointer to a new file data structure. The
 *                          public fields will be initialized by the
 *                          caller, which is always H5FD_open().
 *              Failure:    NULL
 *
 * Programmer:  Robb Matzke
 *              Thursday, July 29, 1999
 *
 *-------------------------------------------------------------------------
 */
static H5FD_t *
H5FD__hermes_open(const char *name, unsigned flags, hid_t fapl_id, haddr_t maxaddr)
{
    H5FD_hermes_t *file = NULL; /* hermes VFD info            */
    int          fd   = -1;   /* File descriptor          */
    int          o_flags;     /* Flags for open() call    */
    h5_stat_t       sb;
    H5P_genplist_t *plist;            /* Property list pointer */
    char *hermes_config = NULL;
    H5FD_t *        ret_value = NULL; /* Return value */

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
        
    /* Initialize hermes here */
    if ((H5OPEN hermes_initialized) == FAIL) {
        hermes_config = HDgetenv(kHermesConf);
        if (HermesInitHermes(hermes_config) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_UNINITIALIZED, NULL, "Hermes initialization failed")
        else
            hermes_initialized = TRUE;
    }

    /* Build the open flags */
    o_flags = (H5F_ACC_RDWR & flags) ? O_RDWR : O_RDONLY;
    if (H5F_ACC_TRUNC & flags)
        o_flags |= O_TRUNC;
    if (H5F_ACC_CREAT & flags)
        o_flags |= O_CREAT;
    if (H5F_ACC_EXCL & flags)
        o_flags |= O_EXCL;

    /* Open the file */  //no???????????????????????
    if ((fd = HDopen(name, o_flags, H5_POSIX_CREATE_MODE_RW)) < 0) {
        int myerrno = errno;
        HGOTO_ERROR(
            H5E_FILE, H5E_CANTOPENFILE, NULL,
            "unable to open file: name = '%s', errno = %d, error message = '%s', flags = %x, o_flags = %x",
            name, myerrno, HDstrerror(myerrno), flags, (unsigned)o_flags);
    } /* end if */

    if (HDfstat(fd, &sb) < 0)
        HSYS_GOTO_ERROR(H5E_FILE, H5E_BADFILE, NULL, "unable to fstat file")

    /* Create the new file struct */
    if (NULL == (file = H5FL_CALLOC(H5FD_hermes_t)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "unable to allocate file struct")

    file->fd = fd;
    H5_CHECKED_ASSIGN(file->eof, haddr_t, sb.st_size, h5_stat_size_t);
    file->pos = HADDR_UNDEF;
    file->op  = OP_UNKNOWN;

    /* Get the FAPL */
    if (NULL == (plist = (H5P_genplist_t *)H5I_object(fapl_id)))
        HGOTO_ERROR(H5E_VFL, H5E_BADTYPE, NULL, "not a file access property list")

#if 0
    /* Check the file locking flags in the fapl */
    if (ignore_disabled_file_locks_s != FAIL)
        /* The environment variable was set, so use that preferentially */
        file->ignore_disabled_file_locks = ignore_disabled_file_locks_s;
    else {
        /* Use the value in the property list */
        if (H5P_get(plist, H5F_ACS_IGNORE_DISABLED_FILE_LOCKS_NAME, &file->ignore_disabled_file_locks) < 0)
            HGOTO_ERROR(H5E_VFL, H5E_CANTGET, NULL, "can't get ignore disabled file locks property")
    }

    /* Retain a copy of the name used to open the file, for possible error reporting */
    HDstrncpy(file->filename, name, sizeof(file->filename));
    file->filename[sizeof(file->filename) - 1] = '\0';

    /* Check for non-default FAPL */
    if (H5P_FILE_ACCESS_DEFAULT != fapl_id) {

        /* This step is for h5repart tool only. If user wants to change file driver from
         * family to one that uses single files (hermes, etc.) while using h5repart, this
         * private property should be set so that in the later step, the library can ignore
         * the family driver information saved in the superblock.
         */
        if (H5P_exist_plist(plist, H5F_ACS_FAMILY_TO_SINGLE_NAME) > 0)
            if (H5P_get(plist, H5F_ACS_FAMILY_TO_SINGLE_NAME, &file->fam_to_single) < 0)
                HGOTO_ERROR(H5E_VFL, H5E_CANTGET, NULL, "can't get property of changing family to single")
    } /* end if */
#endif

    /* Set return value */
    ret_value = (H5FD_t *)file;

done:
    if (NULL == ret_value) {
        if (fd >= 0)
            HDclose(fd);
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
 * Programmer:  Robb Matzke
 *              Thursday, July 29, 1999
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__hermes_close(H5FD_t *_file)
{
    H5FD_hermes_t *file      = (H5FD_hermes_t *)_file;
    herr_t       ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_STATIC

    /* Sanity check */
    HDassert(file);

    /* Close the underlying file */
    if (HDclose(file->fd) < 0)
        HSYS_GOTO_ERROR(H5E_IO, H5E_CANTCLOSEFILE, FAIL, "unable to close file")

    /* Release the file info */
    file = H5FL_FREE(H5FD_hermes_t, file);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__hermes_close() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__hermes_cmp
 *
 * Purpose:     Compares two files belonging to this driver using an
 *              arbitrary (but consistent) ordering.
 *
 * Return:      Success:    A value like strcmp()
 *              Failure:    never fails (arguments were checked by the
 *                          caller).
 *
 * Programmer:  Robb Matzke
 *              Thursday, July 29, 1999
 *
 *-------------------------------------------------------------------------
 */
static int
H5FD__hermes_cmp(const H5FD_t *_f1, const H5FD_t *_f2)
{
    const H5FD_hermes_t *f1        = (const H5FD_hermes_t *)_f1;
    const H5FD_hermes_t *f2        = (const H5FD_hermes_t *)_f2;
    int                ret_value = 0;

    FUNC_ENTER_STATIC_NOERR

done:
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
 * Programmer:  Quincey Koziol
 *              Friday, August 25, 2000
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__hermes_query(const H5FD_t *_file, unsigned long *flags /* out */)
{
    const H5FD_hermes_t *file = (const H5FD_hermes_t *)_file; /* hermes VFD info */

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
        *flags |= H5FD_FEAT_AGGREGATE_METADATA;  /* OK to aggregate metadata allocations  */
        *flags |= H5FD_FEAT_ACCUMULATE_METADATA; /* OK to accumulate metadata for faster writes */
        *flags |= H5FD_FEAT_DATA_SIEVE; /* OK to perform data sieving for faster raw data reads & writes    */
        *flags |= H5FD_FEAT_AGGREGATE_SMALLDATA; /* OK to aggregate "small" raw data allocations */
        *flags |= H5FD_FEAT_POSIX_COMPAT_HANDLE; /* get_handle callback returns a POSIX file descriptor */
        *flags |=
            H5FD_FEAT_SUPPORTS_SWMR_IO; /* VFD supports the single-writer/multiple-readers (SWMR) pattern   */
        *flags |= H5FD_FEAT_DEFAULT_VFD_COMPATIBLE; /* VFD creates a file which can be opened with the default
                                                       VFD      */
     }

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
 * Programmer:  Robb Matzke
 *              Monday, August  2, 1999
 *
 *-------------------------------------------------------------------------
 */
static haddr_t
H5FD__hermes_get_eoa(const H5FD_t *_file, H5FD_mem_t H5_ATTR_UNUSED type)
{
    const H5FD_hermes_t *file = (const H5FD_hermes_t *)_file;

    FUNC_ENTER_STATIC_NOERR
    printf("hermes VFD H5FD__hermes_get_eoa(): eoa = %lld\n", file->eoa);

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
 * Programmer:  Robb Matzke
 *              Thursday, July 29, 1999
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__hermes_set_eoa(H5FD_t *_file, H5FD_mem_t H5_ATTR_UNUSED type, haddr_t addr)
{
    H5FD_hermes_t *file = (H5FD_hermes_t *)_file;

    FUNC_ENTER_STATIC_NOERR
    printf("hermes VFD H5FD__hermes_set_eoa(), addr = %ul\n", addr);

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
 * Programmer:  Robb Matzke
 *              Thursday, July 29, 1999
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
 * Function:       H5FD__hermes_get_handle
 *
 * Purpose:        Returns the file handle of hermes file driver.
 *
 * Returns:        SUCCEED/FAIL
 *
 * Programmer:     Raymond Lu
 *                 Sept. 16, 2002
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__hermes_get_handle(H5FD_t *_file, hid_t H5_ATTR_UNUSED fapl, void **file_handle)
{
    H5FD_hermes_t *file      = (H5FD_hermes_t *)_file;
    herr_t       ret_value = SUCCEED;

    FUNC_ENTER_STATIC

    if (!file_handle)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "file handle not valid")

    *file_handle = &(file->fd);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__hermes_get_handle() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__hermes_read
 *
 * Purpose:     Reads SIZE bytes of data from FILE beginning at address ADDR
 *              into buffer BUF according to data transfer properties in
 *              DXPL_ID.
 *
 * Return:      Success:    SUCCEED. Result is stored in caller-supplied
 *                          buffer BUF.
 *              Failure:    FAIL, Contents of buffer BUF are undefined.
 *
 * Programmer:  Robb Matzke
 *              Thursday, July 29, 1999
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__hermes_read(H5FD_t *_file, H5FD_mem_t H5_ATTR_UNUSED type, hid_t H5_ATTR_UNUSED dxpl_id, haddr_t addr,
                size_t size, void *buf /*out*/)
{
    H5FD_hermes_t *file      = (H5FD_hermes_t *)_file;
    HDoff_t      offset    = (HDoff_t)addr;
    herr_t       ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_STATIC

    HDassert(file && file->pub.cls);
    HDassert(buf);

    /* Check for overflow conditions */
    if (!H5F_addr_defined(addr))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "addr undefined, addr = %llu", (unsigned long long)addr)
    if (REGION_OVERFLOW(addr, size))
        HGOTO_ERROR(H5E_ARGS, H5E_OVERFLOW, FAIL, "addr overflow, addr = %llu", (unsigned long long)addr)

#ifndef H5_HAVE_PREADWRITE
    /* Seek to the correct location (if we don't have pread) */
    if (addr != file->pos || OP_READ != file->op)
        if (HDlseek(file->fd, (HDoff_t)addr, SEEK_SET) < 0)
            HSYS_GOTO_ERROR(H5E_IO, H5E_SEEKERROR, FAIL, "unable to seek to proper position")
#endif /* H5_HAVE_PREADWRITE */

    /* Read data, being careful of interrupted system calls, partial results,
     * and the end of the file.
     */
    while (size > 0) {
        h5_posix_io_t     bytes_in   = 0;  /* # of bytes to read       */
        h5_posix_io_ret_t bytes_read = -1; /* # of bytes actually read */

        /* Trying to read more bytes than the return type can handle is
         * undefined behavior in POSIX.
         */
        if (size > H5_POSIX_MAX_IO_BYTES)
            bytes_in = H5_POSIX_MAX_IO_BYTES;
        else
            bytes_in = (h5_posix_io_t)size;

        do {
#ifdef H5_HAVE_PREADWRITE
            bytes_read = HDpread(file->fd, buf, bytes_in, offset);
            if (bytes_read > 0)
                offset += bytes_read;
#else
            bytes_read  = HDread(file->fd, buf, bytes_in);
#endif /* H5_HAVE_PREADWRITE */
        } while (-1 == bytes_read && EINTR == errno);

        if (-1 == bytes_read) { /* error */
            int    myerrno = errno;
            time_t mytime  = HDtime(NULL);

            offset = HDlseek(file->fd, (HDoff_t)0, SEEK_CUR);

            HGOTO_ERROR(H5E_IO, H5E_READERROR, FAIL,
                        "file read failed: time = %s, filename = '%s', file descriptor = %d, errno = %d, "
                        "error message = '%s', buf = %p, total read size = %llu, bytes this sub-read = %llu, "
                        "bytes actually read = %llu, offset = %llu",
                        HDctime(&mytime), file->filename, file->fd, myerrno, HDstrerror(myerrno), buf,
                        (unsigned long long)size, (unsigned long long)bytes_in,
                        (unsigned long long)bytes_read, (unsigned long long)offset);
        } /* end if */

        if (0 == bytes_read) {
            /* end of file but not end of format address space */
            HDmemset(buf, 0, size);
            break;
        } /* end if */

        HDassert(bytes_read >= 0);
        HDassert((size_t)bytes_read <= size);

        size -= (size_t)bytes_read;
        addr += (haddr_t)bytes_read;
        buf = (char *)buf + bytes_read;
    } /* end while */

    /* Update current position */
    file->pos = addr;
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
 * Purpose:     Writes SIZE bytes of data to FILE beginning at address ADDR
 *              from buffer BUF according to data transfer properties in
 *              DXPL_ID.
 *
 * Return:      SUCCEED/FAIL
 *
 * Programmer:  Robb Matzke
 *              Thursday, July 29, 1999
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__hermes_write(H5FD_t *_file, H5FD_mem_t H5_ATTR_UNUSED type, hid_t H5_ATTR_UNUSED dxpl_id, haddr_t addr,
                 size_t size, const void *buf)
{
    H5FD_hermes_t *file      = (H5FD_hermes_t *)_file;
    HDoff_t      offset    = (HDoff_t)addr;
    herr_t       ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_STATIC
    printf("Calling H5FD__hermes_write()\n");
    printf("size = %ld\n", size);
    printf("mem type is %d\n", type);

    HDassert(file && file->pub.cls);
    HDassert(buf);

    /* Check for overflow conditions */
    if (!H5F_addr_defined(addr))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "addr undefined, addr = %llu", (unsigned long long)addr)
    if (REGION_OVERFLOW(addr, size))
        HGOTO_ERROR(H5E_ARGS, H5E_OVERFLOW, FAIL, "addr overflow, addr = %llu, size = %llu",
                    (unsigned long long)addr, (unsigned long long)size)

#ifndef H5_HAVE_PREADWRITE
    /* Seek to the correct location (if we don't have pwrite) */
    if (addr != file->pos || OP_WRITE != file->op)
        if (HDlseek(file->fd, (HDoff_t)addr, SEEK_SET) < 0)
            HSYS_GOTO_ERROR(H5E_IO, H5E_SEEKERROR, FAIL, "unable to seek to proper position")
#endif /* H5_HAVE_PREADWRITE */

    /* Write the data, being careful of interrupted system calls and partial
     * results
     */
    while (size > 0) {
        h5_posix_io_t     bytes_in    = 0;  /* # of bytes to write  */
        h5_posix_io_ret_t bytes_wrote = -1; /* # of bytes written   */

        /* Trying to write more bytes than the return type can handle is
         * undefined behavior in POSIX.
         */
        if (size > H5_POSIX_MAX_IO_BYTES)
            bytes_in = H5_POSIX_MAX_IO_BYTES;
        else
            bytes_in = (h5_posix_io_t)size;

        do {
#ifdef H5_HAVE_PREADWRITE
            bytes_wrote = HDpwrite(file->fd, buf, bytes_in, offset);
            if (bytes_wrote > 0)
                offset += bytes_wrote;
#else
            bytes_wrote = HDwrite(file->fd, buf, bytes_in);
#endif /* H5_HAVE_PREADWRITE */
        } while (-1 == bytes_wrote && EINTR == errno);

        if (-1 == bytes_wrote) { /* error */
            int    myerrno = errno;
            time_t mytime  = HDtime(NULL);

            offset = HDlseek(file->fd, (HDoff_t)0, SEEK_CUR);

            HGOTO_ERROR(H5E_IO, H5E_WRITEERROR, FAIL,
                        "file write failed: time = %s, filename = '%s', file descriptor = %d, errno = %d, "
                        "error message = '%s', buf = %p, total write size = %llu, bytes this sub-write = "
                        "%llu, bytes actually written = %llu, offset = %llu",
                        HDctime(&mytime), file->filename, file->fd, myerrno, HDstrerror(myerrno), buf,
                        (unsigned long long)size, (unsigned long long)bytes_in,
                        (unsigned long long)bytes_wrote, (unsigned long long)offset);
        } /* end if */

        HDassert(bytes_wrote > 0);
        HDassert((size_t)bytes_wrote <= size);

        size -= (size_t)bytes_wrote;
        addr += (haddr_t)bytes_wrote;
        buf = (const char *)buf + bytes_wrote;
    } /* end while */

    /* Update current position and eof */
    file->pos = addr;
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

/*-------------------------------------------------------------------------
 * Function:    H5FD__hermes_truncate
 *
 * Purpose:     Makes sure that the true file size is the same (or larger)
 *              than the end-of-address.
 *
 * Return:      SUCCEED/FAIL
 *
 * Programmer:  Robb Matzke
 *              Wednesday, August  4, 1999
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__hermes_truncate(H5FD_t *_file, hid_t H5_ATTR_UNUSED dxpl_id, hbool_t H5_ATTR_UNUSED closing)
{
    H5FD_hermes_t *file      = (H5FD_hermes_t *)_file;
    herr_t       ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_STATIC

    HDassert(file);

    /* Extend the file to make sure it's large enough */
    if (!H5F_addr_eq(file->eoa, file->eof)) {
#ifdef H5_HAVE_WIN32_API
        LARGE_INTEGER li;       /* 64-bit (union) integer for SetFilePointer() call */
        DWORD         dwPtrLow; /* Low-order pointer bits from SetFilePointer()
                                 * Only used as an error code here.
                                 */
        DWORD dwError;          /* DWORD error code from GetLastError() */
        BOOL  bError;           /* Boolean error flag */

        /* Windows uses this odd QuadPart union for 32/64-bit portability */
        li.QuadPart = (__int64)file->eoa;

        /* Extend the file to make sure it's large enough.
         *
         * Since INVALID_SET_FILE_POINTER can technically be a valid return value
         * from SetFilePointer(), we also need to check GetLastError().
         */
        dwPtrLow = SetFilePointer(file->hFile, li.LowPart, &li.HighPart, FILE_BEGIN);
        if (INVALID_SET_FILE_POINTER == dwPtrLow) {
            dwError = GetLastError();
            if (dwError != NO_ERROR)
                HGOTO_ERROR(H5E_FILE, H5E_FILEOPEN, FAIL, "unable to set file pointer")
        }

        bError = SetEndOfFile(file->hFile);
        if (0 == bError)
            HGOTO_ERROR(H5E_IO, H5E_SEEKERROR, FAIL, "unable to extend file properly")
#else  /* H5_HAVE_WIN32_API */
        if (-1 == HDftruncate(file->fd, (HDoff_t)file->eoa))
            HSYS_GOTO_ERROR(H5E_IO, H5E_SEEKERROR, FAIL, "unable to extend file properly")
#endif /* H5_HAVE_WIN32_API */

        /* Update the eof value */
        file->eof = file->eoa;

        /* Reset last file I/O information */
        file->pos = HADDR_UNDEF;
        file->op  = OP_UNKNOWN;
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__hermes_truncate() */
