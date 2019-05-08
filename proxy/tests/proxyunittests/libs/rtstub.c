#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
//#include <linux/list.h>
#include <memory.h>
#include <errno.h>
#include <limits.h>
#include <sys/mman.h>

#define FUNC_ADDRLEN_MAX 32

typedef void (*func_samp)(void);

#ifndef offsetof
#define offsetof(TYPE, MEMBER) ((size_t) &((TYPE *)0)->MEMBER)
#endif

//#define offsetof(TYPE, MEMBER) ((size_t) &((TYPE *)0)->MEMBER)
/**
 * container_of - cast a member of a structure out to the containing structure
 * @ptr:        the pointer to the member.
 * @type:       the type of the container struct this is embedded in.
 * @member:     the name of the member within the struct.
 *
 */
#define container_of(ptr, type, member) ({         \
    const typeof( ((type *)0)->member ) *__mptr = (ptr);        \
   (type *)( (char *)__mptr - offsetof(type, member) );})


/*==============================================================
****************** Definitions of list interfaces **************
**============================================================*/
#ifndef _LINUX_LIST_H
#define _LINUX_LIST_H

/********** include/linux/list.h **********/
/*
 * These are non-NULL pointers that will result in page faults
 * under normal circumstances, used to verify that nobody uses
 * non-initialized list entries.
 */
#define LIST_POISON1  ((void *) 0x00100100)
#define LIST_POISON2  ((void *) 0x00200200)

#define container_of_pointer(ptr, type, member) ({                      \
    const typeof( ((type *)0)->member ) __mptr = (ptr); \
    (type *)( (char *)__mptr - offsetof(type, member) );})

static inline void prefetch(void *x) 
{ 
    (void)x;
    //asm volatile("prefetcht0 %0" :: "m" (*(unsigned long *)x));
} 

/**
  * list usage copied from kernel 
  */
/*
 * Simple doubly linked list implementation.
 *
 * Some of the internal functions ("__xxx") are useful when
 * manipulating whole lists rather than single entries, as
 * sometimes we already know the next/prev entries and we can
 * generate better code by using them directly rather than
 * using the generic single-entry routines.
 */

struct list_head {
    struct list_head *next;
    struct list_head *prev;
};

#define LIST_HEAD_INIT(name) { &(name), &(name) }

#define LIST_HEAD(name) \
    struct list_head name = LIST_HEAD_INIT(name)

static inline void INIT_LIST_HEAD(struct list_head *list)
{
    list->next = list;
    list->prev = list;
}

/*
 * Insert a new entry between two known consecutive entries.
 *
 * This is only for internal list manipulation where we know
 * the prev/next entries already!
 */
static inline void __list_add(struct list_head *new,
                  struct list_head *prev,
                  struct list_head *next)
{
    next->prev = new;
    new->next = next;
    new->prev = prev;
    prev->next = new;
}

/**
 * list_add - add a new entry
 * @new: new entry to be added
 * @head: list head to add it after
 *
 * Insert a new entry after the specified head.
 * This is good for implementing stacks.
 */
static inline void list_add_head(struct list_head *new, struct list_head *head)
{
    __list_add(new, head, head->next);
}

/**
 * list_add_tail - add a new entry
 * @new: new entry to be added
 * @head: list head to add it before
 *
 * Insert a new entry before the specified head.
 * This is useful for implementing queues.
 */
static inline void list_add_tail(struct list_head *new, struct list_head *head)
{
    __list_add(new, head->prev, head);
}

/*
 * Delete a list entry by making the prev/next entries
 * point to each other.
 *
 * This is only for internal list manipulation where we know
 * the prev/next entries already!
 */
static inline void __list_del(struct list_head * prev, struct list_head * next)
{
    next->prev = prev;
    prev->next = next;
}

/**
 * list_del - deletes entry from list.
 * @entry: the element to delete from the list.
 * Note: list_empty on entry does not return true after this, the entry is
 * in an undefined state.
 */
static inline void list_del(struct list_head *entry)
{
    __list_del(entry->prev, entry->next);
    entry->next = LIST_POISON1;
    entry->prev = LIST_POISON2;
}

/**
 * list_replace - replace old entry by new one
 * @old : the element to be replaced
 * @new : the new element to insert
 * Note: if 'old' was empty, it will be overwritten.
 */
static inline void list_replace(struct list_head *old,
                                struct list_head *new)
{
    new->next = old->next;
    new->next->prev = new;
    new->prev = old->prev;
    new->prev->next = new;
}

static inline void list_replace_init(struct list_head *old,
                                        struct list_head *new)
{
    list_replace(old, new);
    INIT_LIST_HEAD(old);
}

/**
 * list_del_init - deletes entry from list and reinitialize it.
 * @entry: the element to delete from the list.
 */
static inline void list_del_init(struct list_head *entry)
{
    __list_del(entry->prev, entry->next);
    INIT_LIST_HEAD(entry);
}

/**
 * list_move - delete from one list and add as another's head
 * @list: the entry to move
 * @head: the head that will precede our entry
 */
static inline void list_move(struct list_head *list, struct list_head *head)
{
    __list_del(list->prev, list->next);
    list_add_head(list, head);
}

/**
 * list_move_tail - delete from one list and add as another's tail
 * @list: the entry to move
 * @head: the head that will follow our entry
 */
static inline void list_move_tail(struct list_head *list,
                                  struct list_head *head)
{
    __list_del(list->prev, list->next);
    list_add_tail(list, head);
}

/**
 * list_is_last - tests whether @list is the last entry in list @head
 * @list: the entry to test
 * @head: the head of the list
 */
static inline int list_is_last(const struct list_head *list,
                                const struct list_head *head)
{
    return list->next == head;
}

/**
 * list_empty - tests whether a list is empty
 * @head: the list to test.
 */
static inline int list_empty(const struct list_head *head)
{
    return head->next == head;
}

/**
 * list_empty_careful - tests whether a list is empty and not being modified
 * @head: the list to test
 *
 * Description:
 * tests whether a list is empty _and_ checks that no other CPU might be
 * in the process of modifying either member (next or prev)
 *
 * NOTE: using list_empty_careful() without synchronization
 * can only be safe if the only activity that can happen
 * to the list entry is list_del_init(). Eg. it cannot be used
 * if another CPU could re-list_add() it.
 */
static inline int list_empty_careful(const struct list_head *head)
{
    struct list_head *next = head->next;
    return (next == head) && (next == head->prev);
}

static inline void __list_splice(struct list_head *list,
                                 struct list_head *head)
{
    struct list_head *first = list->next;
    struct list_head *last = list->prev;
    struct list_head *at = head->next;

    first->prev = head;
    head->next = first;

    last->next = at;
    at->prev = last;
}

/**
 * list_splice - join two lists
 * @list: the new list to add.
 * @head: the place to add it in the first list.
 */
static inline void list_splice(struct list_head *list, struct list_head *head)
{
    if (!list_empty(list))
    __list_splice(list, head);
}

/**
 * list_splice_init - join two lists and reinitialise the emptied list.
 * @list: the new list to add.
 * @head: the place to add it in the first list.
 *
 * The list at @list is reinitialised
 */
static inline void list_splice_init(struct list_head *list,
                                    struct list_head *head)
{
    if (!list_empty(list)) {
    __list_splice(list, head);
    INIT_LIST_HEAD(list);
    }
}

/**
 * list_entry - get the struct for this entry
 * @ptr:        the &struct list_head pointer.
 * @type:       the type of the struct this is embedded in.
 * @member:     the name of the list_struct within the struct.
 */
#define list_entry(ptr, type, member) \
    container_of(ptr, type, member)

/**
 * list_for_each        -       iterate over a list
 * @pos:        the &struct list_head to use as a loop cursor.
 * @head:       the head for your list.
 */
#define list_for_each(pos, head) \
    for (pos = (head)->next; prefetch(pos->next), pos != (head); \
     pos = pos->next)

/**
 * __list_for_each      -       iterate over a list
 * @pos:        the &struct list_head to use as a loop cursor.
 * @head:       the head for your list.
 *
 * This variant differs from list_for_each() in that it's the
 * simplest possible list iteration code, no prefetching is done.
 * Use this for code that knows the list to be very short (empty
 * or 1 entry) most of the time.
 */
#define __list_for_each(pos, head) \
    for (pos = (head)->next; pos != (head); pos = pos->next)

/**
 * list_for_each_prev   -       iterate over a list backwards
 * @pos:        the &struct list_head to use as a loop cursor.
 * @head:       the head for your list.
 */
#define list_for_each_prev(pos, head) \
    for (pos = (head)->prev; prefetch(pos->prev), pos != (head); \
    pos = pos->prev)

/**
 * list_for_each_safe - iterate over a list safe against removal of list entry
 * @pos:        the &struct list_head to use as a loop cursor.
 * @n:          another &struct list_head to use as temporary storage
 * @head:       the head for your list.
 */
#define list_for_each_safe(pos, n, head) \
        for (pos = (head)->next, n = pos->next; pos != (head); \
                pos = n, n = pos->next)

/**
 * list_for_each_entry  -       iterate over list of given type
 * @pos:        the type * to use as a loop cursor.
 * @head:       the head for your list.
 * @member:     the name of the list_struct within the struct.
 */
#define list_for_each_entry(pos, head, member)                          \
    for (pos = list_entry((head)->next, typeof(*pos), member);  \
     prefetch(pos->member.next), &pos->member != (head);        \
     pos = list_entry(pos->member.next, typeof(*pos), member))

/**
 * list_for_each_entry_reverse - iterate backwards over list of given type.
 * @pos:        the type * to use as a loop cursor.
 * @head:       the head for your list.
 * @member:     the name of the list_struct within the struct.
 */
#define list_for_each_entry_reverse(pos, head, member)                  \
    for (pos = list_entry((head)->prev, typeof(*pos), member);  \
     prefetch(pos->member.prev), &pos->member != (head);        \
     pos = list_entry(pos->member.prev, typeof(*pos), member))

/**
 * list_prepare_entry - prepare a pos entry for use in list_for_each_entry_continue
 * @pos:        the type * to use as a start point
 * @head:       the head of the list
 * @member:     the name of the list_struct within the struct.
 *
 * Prepares a pos entry for use as a start point in list_for_each_entry_continue.
 */
#define list_prepare_entry(pos, head, member) \
    ((pos) ? : list_entry(head, typeof(*pos), member))

/**
 * list_for_each_entry_continue - continue iteration over list of given type
 * @pos:        the type * to use as a loop cursor.
 * @head:       the head for your list.
 * @member:     the name of the list_struct within the struct.
 *
 * Continue to iterate over list of given type, continuing after
 * the current position.
 */
#define list_for_each_entry_continue(pos, head, member)                 \
    for (pos = list_entry(pos->member.next, typeof(*pos), member);      \
     prefetch(pos->member.next), &pos->member != (head);        \
     pos = list_entry(pos->member.next, typeof(*pos), member))

/**
 * list_for_each_entry_from - iterate over list of given type from the current point
 * @pos:        the type * to use as a loop cursor.
 * @head:       the head for your list.
 * @member:     the name of the list_struct within the struct.
 *
 * Iterate over list of given type, continuing from current position.
 */
#define list_for_each_entry_from(pos, head, member)                     \
    for (; prefetch(pos->member.next), &pos->member != (head);  \
     pos = list_entry(pos->member.next, typeof(*pos), member))

/**
 * list_for_each_entry_safe - iterate over list of given type safe against removal of list entry
 * @pos:        the type * to use as a loop cursor.
 * @n:          another type * to use as temporary storage
 * @head:       the head for your list.
 * @member:     the name of the list_struct within the struct.
 */
#define list_for_each_entry_safe(pos, n, head, member)                  \
    for (pos = list_entry((head)->next, typeof(*pos), member),  \
     n = list_entry(pos->member.next, typeof(*pos), member);    \
     &pos->member != (head);                                    \
     pos = n, n = list_entry(n->member.next, typeof(*n), member))

/**
 * list_for_each_entry_safe_continue
 * @pos:        the type * to use as a loop cursor.
 * @n:          another type * to use as temporary storage
 * @head:       the head for your list.
 * @member:     the name of the list_struct within the struct.
 *
 * Iterate over list of given type, continuing after current point,
 * safe against removal of list entry.
 */
#define list_for_each_entry_safe_continue(pos, n, head, member)                 \
    for (pos = list_entry(pos->member.next, typeof(*pos), member),              \
     n = list_entry(pos->member.next, typeof(*pos), member);            \
     &pos->member != (head);                                            \
     pos = n, n = list_entry(n->member.next, typeof(*n), member))

/**
 * list_for_each_entry_safe_from
 * @pos:        the type * to use as a loop cursor.
 * @n:          another type * to use as temporary storage
 * @head:       the head for your list.
 * @member:     the name of the list_struct within the struct.
 *
 * Iterate over list of given type from current point, safe against
 * removal of list entry.
 */
#define list_for_each_entry_safe_from(pos, n, head, member)                     \
    for (n = list_entry(pos->member.next, typeof(*pos), member);                \
     &pos->member != (head);                                            \
     pos = n, n = list_entry(n->member.next, typeof(*n), member))

/**
 * list_for_each_entry_safe_reverse
 * @pos:        the type * to use as a loop cursor.
 * @n:          another type * to use as temporary storage
 * @head:       the head for your list.
 * @member:     the name of the list_struct within the struct.
 *
 * Iterate backwards over list of given type, safe against removal
 * of list entry.
 */
#define list_for_each_entry_safe_reverse(pos, n, head, member)          \
    for (pos = list_entry((head)->prev, typeof(*pos), member),  \
     n = list_entry(pos->member.prev, typeof(*pos), member);    \
     &pos->member != (head);                                    \
     pos = n, n = list_entry(n->member.prev, typeof(*n), member))

#endif
/*==============================================================
****************** Definitions of trie tree ********************
**============================================================*/
#ifndef _LINUX_RBTREE_H
#define _LINUX_RBTREE_H

struct rb_node
{
    struct rb_node *rb_parent;
    int rb_color;
#define RB_RED          0
#define RB_BLACK        1
    struct rb_node *rb_right;
    struct rb_node *rb_left;
};

struct rb_root
{
    struct rb_node *rb_node;
};

#define RB_ROOT { NULL }
#define rb_entry(ptr, type, member)                                     \
    ((type *)((char *)(ptr)-(unsigned long)(&((type *)0)->member)))

extern void rb_insert_color(struct rb_node *, struct rb_root *);
extern void rb_erase(struct rb_node *, struct rb_root *);

/* Find logical next and previous nodes in a tree */
extern struct rb_node *rb_next(struct rb_node *);
extern struct rb_node *rb_prev(struct rb_node *);
extern struct rb_node *rb_first(struct rb_root *);

/* Fast replacement of a single node without remove/rebalance/add/rebalance */
extern void rb_replace_node(struct rb_node *victim, struct rb_node *new, 
                            struct rb_root *root);

static void rb_link_node(struct rb_node * node, struct rb_node * parent,
             struct rb_node ** rb_link)
{
    node->rb_parent = parent;
    node->rb_color = RB_RED;
        node->rb_left = node->rb_right = NULL;

        *rb_link = node;
}

/*==============================================================
******************* Interfaces of trie tree ********************
**============================================================*/

static void __rb_rotate_left(struct rb_node *node, struct rb_root *root)
{
    struct rb_node *right = node->rb_right;

    if ((node->rb_right = right->rb_left))
    {
        right->rb_left->rb_parent = node;
    }
    right->rb_left = node;

    if ((right->rb_parent = node->rb_parent))
    {
        if (node == node->rb_parent->rb_left)
        {
            node->rb_parent->rb_left = right;
        }
    else
    {
        node->rb_parent->rb_right = right;
    }
        }
    else
    {
        root->rb_node = right;
    }
    node->rb_parent = right;
}

static void __rb_rotate_right(struct rb_node *node, struct rb_root *root)
{
    struct rb_node *left = node->rb_left;

    if ((node->rb_left = left->rb_right))
    {
        left->rb_right->rb_parent = node;
    }
    left->rb_right = node;

    if ((left->rb_parent = node->rb_parent))
    {
        if (node == node->rb_parent->rb_right)
        {
        node->rb_parent->rb_right = left;
        }
        else
        {
        node->rb_parent->rb_left = left;
        }
    }
    else
    {
        root->rb_node = left;
    }
    node->rb_parent = left;
}

void rb_insert_color(struct rb_node *node, struct rb_root *root)
{
    struct rb_node *parent = NULL;
    struct rb_node *gparent = NULL;

    while ((parent = node->rb_parent) && parent->rb_color == RB_RED)
    {
        gparent = parent->rb_parent;

        if (parent == gparent->rb_left)
        {
            {
                register struct rb_node *uncle = gparent->rb_right;
                if (uncle && uncle->rb_color == RB_RED)
                {
                    uncle->rb_color = RB_BLACK;
                    parent->rb_color = RB_BLACK;
                    gparent->rb_color = RB_RED;
                    node = gparent;
                    continue;
                }
            }

            if (parent->rb_right == node)
            {
                register struct rb_node *tmp;
                __rb_rotate_left(parent, root);
                tmp = parent;
                parent = node;
                node = tmp;
            }

            parent->rb_color = RB_BLACK;
            gparent->rb_color = RB_RED;
            __rb_rotate_right(gparent, root);
        } else {
            {
                register struct rb_node *uncle = gparent->rb_left;
                if (uncle && uncle->rb_color == RB_RED)
                {
                    uncle->rb_color = RB_BLACK;
                    parent->rb_color = RB_BLACK;
                    gparent->rb_color = RB_RED;
                    node = gparent;
                    continue;
                }
            }

            if (parent->rb_left == node)
            {
                register struct rb_node *tmp;
                __rb_rotate_right(parent, root);
                tmp = parent;
                parent = node;
                node = tmp;
            }

            parent->rb_color = RB_BLACK;
            gparent->rb_color = RB_RED;
            __rb_rotate_left(gparent, root);
        }
    }

    root->rb_node->rb_color = RB_BLACK;
}

static void __rb_erase_color(struct rb_node *node, struct rb_node *parent,
                             struct rb_root *root)
{
    struct rb_node *other;

    while ((!node || node->rb_color == RB_BLACK) && node != root->rb_node)
    {
        if (parent->rb_left == node)
        {
            other = parent->rb_right;
            if (other->rb_color == RB_RED)
            {
                other->rb_color = RB_BLACK;
                parent->rb_color = RB_RED;
                __rb_rotate_left(parent, root);
               other = parent->rb_right;
            }
            if ((!other->rb_left || other->rb_left->rb_color == RB_BLACK)
                    && (!other->rb_right || other->rb_right->rb_color == RB_BLACK))
            {
                other->rb_color = RB_RED;
                node = parent;
                parent = node->rb_parent;
            }
            else
            {
                if (!other->rb_right ||
                        other->rb_right->rb_color == RB_BLACK)
                {
                    register struct rb_node *o_left;
                    if ((o_left = other->rb_left))
                    {
                        o_left->rb_color = RB_BLACK;
                    }
                    other->rb_color = RB_RED;
                    __rb_rotate_right(other, root);
                    other = parent->rb_right;
                }
                other->rb_color = parent->rb_color;
                parent->rb_color = RB_BLACK;
                if (other->rb_right)
                {
                    other->rb_right->rb_color = RB_BLACK;
                }
                __rb_rotate_left(parent, root);
                node = root->rb_node;
                break;
            }
        }
        else
        {
            other = parent->rb_left;
            if (other->rb_color == RB_RED)
            {
                other->rb_color = RB_BLACK;
                parent->rb_color = RB_RED;
                __rb_rotate_right(parent, root);
                other = parent->rb_left;
            }
            if ((!other->rb_left ||
                    other->rb_left->rb_color == RB_BLACK)
                        && (!other->rb_right ||
                            other->rb_right->rb_color == RB_BLACK))
            {
                other->rb_color = RB_RED;
                node = parent;
                parent = node->rb_parent;
            }
            else
            {
                if (!other->rb_left ||
                        other->rb_left->rb_color == RB_BLACK)
                {
                    register struct rb_node *o_right;
                    if ((o_right = other->rb_right))
                    {
                        o_right->rb_color = RB_BLACK;
                    }
                    other->rb_color = RB_RED;
                    __rb_rotate_left(other, root);
                    other = parent->rb_left;
                }
                other->rb_color = parent->rb_color;
                parent->rb_color = RB_BLACK;
                if (other->rb_left)
                {
                    other->rb_left->rb_color = RB_BLACK;
                }
                __rb_rotate_right(parent, root);
                node = root->rb_node;
                break;
            }
        }
    }
    if (node)
        node->rb_color = RB_BLACK;
}

void rb_erase(struct rb_node *node, struct rb_root *root)
{
    struct rb_node *child = NULL;
    struct rb_node *parent = NULL;
    int color = RB_BLACK;

    if (!node->rb_left)
    {
        child = node->rb_right;
    }
    else if (!node->rb_right)
    {
        child = node->rb_left;
    }
    else
    {
        struct rb_node *old = node;
        struct rb_node *left = NULL;
        node = node->rb_right;
        while ((left = node->rb_left))
        {
            node = left;
        }
        child = node->rb_right;
        parent = node->rb_parent;
        color = node->rb_color;

        if (child)
        {
            child->rb_parent = parent;
        }
        if (parent)
        {
            if (parent->rb_left == node)
                parent->rb_left = child;
            else
                parent->rb_right = child;
        }
        else
        {
            root->rb_node = child;
        }
        if (node->rb_parent == old)
        {
            parent = node;
        }
        node->rb_parent = old->rb_parent;
        node->rb_color = old->rb_color;
        node->rb_right = old->rb_right;
        node->rb_left = old->rb_left;
        if (old->rb_parent)
        {
            if (old->rb_parent->rb_left == old)
            {
                old->rb_parent->rb_left = node;
            }
            else
            {
                old->rb_parent->rb_right = node;
            }
        } 
        else
        {
            root->rb_node = node;
        }
        old->rb_left->rb_parent = node;
        if (old->rb_right)
        {
            old->rb_right->rb_parent = node;
        }
                
        if (color == RB_BLACK)
        {
            __rb_erase_color(child, parent, root);
        }
        return;
    }

    parent = node->rb_parent;
    color = node->rb_color;
    if (child)
    {
        child->rb_parent = parent;
    }
        
    if (parent)
    {
        if (parent->rb_left == node)
        {
            parent->rb_left = child;
        }
        else
        {
            parent->rb_right = child;
        }
    }
    else
    {
        root->rb_node = child;
    }

    if (color == RB_BLACK)
    {
        __rb_erase_color(child, parent, root);
    }
}

/*
 * This function returns the first node (in sort order) of the tree.
 */
struct rb_node *rb_first(struct rb_root *root)
{
    struct rb_node  *n;

    n = root->rb_node;
    if (!n)
    {
        return 0;
    }
       
    while (n->rb_left)
    {
        n = n->rb_left;
    }
        
    return n;
}

struct rb_node *rb_next(struct rb_node *node)
{
    /* If we have a right-hand child, go down and then left as far
    as we can. */
    if (node->rb_right) 
    {
        node = node->rb_right; 
        while (node->rb_left)
        {
            node=node->rb_left;
        }
               
        return node;
    }

    /* No right-hand children.  Everything down and left is
       smaller than us, so any 'next' node must be in the general
       direction of our parent. Go up the tree; any time the
       ancestor is a right-hand child of its parent, keep going
       up. First time it's a left-hand child of its parent, said
       parent is our 'next' node. */
    while (node->rb_parent && node == node->rb_parent->rb_right)
    {
        node = node->rb_parent;
    }

    return node->rb_parent;
}

struct rb_node *rb_prev(struct rb_node *node)
{
    /* If we have a left-hand child, go down and then right as far
       as we can. */
    if (node->rb_left) 
    {
        node = node->rb_left; 
        while (node->rb_right)
        {
            node=node->rb_right;
        }
        return node;
    }

    /* No left-hand children. Go up till we find an ancestor which
       is a right-hand child of its parent */
    while (node->rb_parent && node == node->rb_parent->rb_left)
    {
        node = node->rb_parent;
    }

    return node->rb_parent;
}

void rb_replace_node(struct rb_node *victim, struct rb_node *new,
                     struct rb_root *root)
{
    struct rb_node *parent = victim->rb_parent;

    /* Set the surrounding nodes to point to the replacement */
    if (parent) {
        if (victim == parent->rb_left)
        {
            parent->rb_left = new;
        }
        else
        {
            parent->rb_right = new;
        }
    }
    else 
    {
        root->rb_node = new;
    }
    if (victim->rb_left)
    {
        victim->rb_left->rb_parent = new;
    }
    if (victim->rb_right)
    {
        victim->rb_right->rb_parent = new;
    }

    /* Copy the pointers/colour from the victim to the replacement */
    *new = *victim;
}
#endif

/*==============================================================
****************** INTERFACES FOR FUNC STUB ********************
**============================================================*/
#define FUNC_ADDR_LEN 32
#define CODESIZE 5U

typedef struct stat_head {
    struct list_head list;
    unsigned int count;
} stat_head_t;

typedef struct func_jumpdesc
{
    void *func;
    unsigned char funccode_back[CODESIZE];
} func_jumpdesc_t;

typedef struct funcjump_node {
    struct rb_node jumprbnode;
    struct list_head list;
    char func_addr[FUNC_ADDR_LEN];
    int stubed;
    func_jumpdesc_t func_jump;
} funcjump_node_t;

typedef struct func_stub_stat {
    struct rb_root rbroot;
    stat_head_t stub_stat; 
} func_stub_stat_t;

void funcstub_stat_init(stat_head_t *pstathead)
{
    INIT_LIST_HEAD(&pstathead->list);
    pstathead->count = 0;
}

void funcstub_stat_add(stat_head_t *pstathead, funcjump_node_t *pfuncjumpnode)
{
    list_add_tail(&pfuncjumpnode->list, &pstathead->list);
    pstathead->count++;
}

void funcstub_stat_del(stat_head_t *pstathead, funcjump_node_t *pfuncjumpnode)
{
    list_del_init(&pfuncjumpnode->list);
    pstathead->count--;
}

void func_display(func_stub_stat_t *pstat)
{
    int cmpres = 0;
    funcjump_node_t *funcjumpnode = NULL; 
    funcjump_node_t *tmpjumpnode = NULL;

    list_for_each_entry_safe(funcjumpnode, tmpjumpnode, &pstat->stub_stat.list, list)
    {
        printf("get rbnode(addr:%x left:%x right:%x) which record func(%x %s) "
               "with stub mark:%d\n",
               funcjumpnode, 
               funcjumpnode->jumprbnode.rb_left,
               funcjumpnode->jumprbnode.rb_right,
               funcjumpnode->func_jump.func,
               funcjumpnode->func_addr, 
               funcjumpnode->stubed);
    }
}

funcjump_node_t *func_search(struct rb_root *root, char *funcaddr)
{
    int cmpres = 0;
    funcjump_node_t *funcjumpnode = NULL;   
    struct rb_node *rbnode = root->rb_node;

    while (rbnode) 
    {
        funcjumpnode = container_of(rbnode, funcjump_node_t, jumprbnode);

        printf("get rbnode(addr:%x left:%x right:%x) which record func(%x %s) "
               "with stub mark:%d\n",
               funcjumpnode, 
               funcjumpnode->jumprbnode.rb_left,
               funcjumpnode->jumprbnode.rb_right,
               funcjumpnode->func_jump.func,
               funcjumpnode->func_addr, funcjumpnode->stubed);
        cmpres = strcmp(funcaddr, funcjumpnode->func_addr);
        if (cmpres < 0)
        {
            rbnode = rbnode->rb_left;
        }
        else if (cmpres > 0)
        {
            rbnode = rbnode->rb_right;
        }
        else
        {
            return funcjumpnode;
        }
    }
        
    return NULL;
}

void space_free(void **pptr)
{
    if (pptr && *pptr) 
    {
        free(*pptr);
        *pptr = NULL;
    }
}

int funcjumpnode_alloc(funcjump_node_t **ppfuncjumpnode)
{
    int ret = 0;
    funcjump_node_t *pfuncjumpnode = NULL;

    pfuncjumpnode = (funcjump_node_t *)malloc(sizeof(funcjump_node_t));
    if (!pfuncjumpnode)
    {
        ret = -1;
        printf("allocate funcjump_node_t failed\n");
        return ret;
    }

    memset(pfuncjumpnode, 0, sizeof(funcjump_node_t));
    INIT_LIST_HEAD(&pfuncjumpnode->list);

    *ppfuncjumpnode = pfuncjumpnode;

    return ret;
}

void funcjumpnode_delete(func_stub_stat_t *pstat, funcjump_node_t *pfuncjumpnode)
{
    rb_erase(&pfuncjumpnode->jumprbnode, &pstat->rbroot);
    funcstub_stat_del(&pstat->stub_stat, pfuncjumpnode);

    space_free((void **)&pfuncjumpnode);
}

func_stub_stat_t g_stubstat;
static long pagesize = -1;

static inline void *pageof(const void* p)
{ 
    return (void *)((unsigned long)p & ~(pagesize - 1));
}

int pagesize_init(void)
{
    int ret = 0;

    pagesize = sysconf(_SC_PAGE_SIZE);

    if (pagesize < 0)
    {
        perror("get system _SC_PAGE_SIZE configure failed");
        ret = -1;
    }
                
    return ret;
}

int rtstub_srcfuncbak(func_samp *func, 
                      char *funcaddr,
                      funcjump_node_t **ppfuncjumpnode)
{
    int ret = 0;
    funcjump_node_t *pfuncjumpnode = NULL;

    ret = funcjumpnode_alloc(&pfuncjumpnode);
    if (ret < 0)
    {
        printf("funcjumpnode_alloc failed\n");
	    return ret;
    }

    snprintf(pfuncjumpnode->func_addr, FUNC_ADDR_LEN, "%s", funcaddr);
    pfuncjumpnode->func_jump.func = (void *)func;
    memcpy(pfuncjumpnode->func_jump.funccode_back, func, CODESIZE);

    *ppfuncjumpnode = pfuncjumpnode;

    return ret;
}

void rtstub_dojump(func_samp *func,func_samp *stub_func)
{
    int ret = 0;

    ret = mprotect(pageof(func), pagesize * 2, 
                               PROT_READ | PROT_WRITE | PROT_EXEC);
    if (-1 == ret)
    {
        printf("mprotect to w+r+x faild:%d", errno);
        exit(errno);
    }

    *(unsigned char *)func = (unsigned char)0xE9;
    *(unsigned int *)((unsigned char *)func + 1) = 
                        (unsigned char *)stub_func - (unsigned char *)func - CODESIZE;

    ret = mprotect(pageof(func), pagesize * 2, PROT_READ | PROT_EXEC);
    if (-1 == ret)
    {
        printf("mprotect to r+x failed:%d", errno);
        exit(errno);
    }
}

void rtstub_dojumpback(funcjump_node_t *pfuncjumpnode)
{
    int ret = 0;
    void *func = pfuncjumpnode->func_jump.func;

    if (!func)
    {
        printf("WARNING: rtstub_dojumpback func is null\n");
        return;
    }

    ret = mprotect(pageof(func), pagesize * 2, 
                            PROT_READ | PROT_WRITE | PROT_EXEC);
    if (-1 == ret)
    {
        perror("mprotect to w+r+x faild");
        exit(errno);
    }

    memcpy(func, pfuncjumpnode->func_jump.funccode_back, CODESIZE);

    ret = mprotect(pageof(func), pagesize * 2, PROT_READ | PROT_EXEC);
    if (-1 == ret)
    {
        perror("mprotect to r+x failed");
        exit(errno);
    }

    //memset(&pfuncjumpnode->func_jump, 0, sizeof(pfuncjumpnode->func_jump));
    pfuncjumpnode->stubed = 0;
}

int funcjumpnode_insert(func_stub_stat_t *pstubstat,
                        func_samp *func,
                        char *funcaddr,
                        funcjump_node_t **ppfuncjumpnode)
{
    int ret = 0;
    int cmpres = 0;
    struct rb_node **new = &(pstubstat->rbroot.rb_node);
    struct rb_node *parent = NULL;
    funcjump_node_t *curfuncjumpnode = NULL;
    funcjump_node_t *ptmpfuncjumpnode = NULL;

    /* Figure out where to put new node */
    while (*new) 
    {
        curfuncjumpnode = container_of(*new, funcjump_node_t, jumprbnode);
        cmpres = strcmp(curfuncjumpnode->func_addr, funcaddr);

        parent = *new;
        if (cmpres < 0)
        {
            new = &((*new)->rb_left);
        }
        else if (cmpres > 0)
        {
            new = &((*new)->rb_right);
        }
        else
        {
            *ppfuncjumpnode = curfuncjumpnode;
        
            if (curfuncjumpnode->stubed != 0)
            {
                ret = -1;
                printf("func:%s already stubed\n", funcaddr);
            }

            return ret;
        }
    }

    ret = rtstub_srcfuncbak(func, funcaddr, ppfuncjumpnode);
    if (ret < 0)
    {
        printf("rtstub_srcfuncbak failed\n");
        return ret;
    }

    /* Add new node and rebalance tree. */    
    ptmpfuncjumpnode = *ppfuncjumpnode;
    rb_link_node(&ptmpfuncjumpnode->jumprbnode, parent, new);
    rb_insert_color(&ptmpfuncjumpnode->jumprbnode, &pstubstat->rbroot);

    funcstub_stat_add(&pstubstat->stub_stat, ptmpfuncjumpnode);

    printf("add record for func:%s to be stubed info rbnode:%x\n",
           funcaddr, *ppfuncjumpnode);
    func_display(pstubstat);

    return ret;
}

int rtstub_create(func_samp *func,func_samp *stub_func)
{
    int ret = 0;
    char funcaddr[FUNC_ADDR_LEN];
    funcjump_node_t *pfuncjumpnode = NULL;

    if (!func)
    {
        printf("WARNING:null func ptr\n");
        return ret;
    }

    snprintf(funcaddr, FUNC_ADDR_LEN, "%x", func);

    ret = funcjumpnode_insert(&g_stubstat, func, funcaddr, &pfuncjumpnode);
    if (ret < 0)
    {
        printf("funcjumpnode_insert failed\n");
        return ret;
    }

    pfuncjumpnode->stubed = 1;
    rtstub_dojump(func, stub_func);

    return ret;
}

void rtstub_destroy(func_samp *func)
{
    char funcaddr[FUNC_ADDR_LEN];
    funcjump_node_t *pfuncjumpnode = NULL;

    snprintf(funcaddr, FUNC_ADDR_LEN, "%x", func);
    pfuncjumpnode = func_search(&g_stubstat.rbroot, funcaddr);
    if (!pfuncjumpnode)
    {
        printf("not found func(%s)\n", funcaddr);
    }
    else
    {
        rtstub_dojumpback(pfuncjumpnode);
    }
}

int rtstub_init(void)
{       
    int ret = 0;

    ret = pagesize_init();
    if (ret < 0)
    {
        printf("pagesize init failed\n");
        return ret;
    }

    memset(&g_stubstat, 0, sizeof(g_stubstat));
    funcstub_stat_init(&g_stubstat.stub_stat);

    return ret;
}

void rtstub_finalize(void)
{
    funcjump_node_t *pfuncjummpnode = NULL;
    funcjump_node_t *tmp = NULL;

    list_for_each_entry_safe(pfuncjummpnode, tmp, &g_stubstat.stub_stat.list, list)
    {
        funcjumpnode_delete(&g_stubstat, pfuncjummpnode);
    }
}

