/***************************************************************************
 * 
 * Copyright (c) 2015 gmail.com, Inc. All Rights Reserved
 * 
 **************************************************************************/

/**
 * @file trietree.h
 * @author taolizao33@gmail.com
 *
 */
 
#ifndef DBA_REDIS_DBCACHECONV_TRIETREE_H
#define DBA_REDIS_DBCACHECONV_TRIETREE_H

enum {
    RB_RED = 0,
    RB_BLACK = 1,
};

struct rb_node
{
    struct rb_node *rb_parent;
    int rb_color;
//#define RB_RED          0
//#define RB_BLACK        1
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

/* Find logical next and previous nodes in a tree */
struct rb_node *rb_next(struct rb_node *);
struct rb_node *rb_prev(struct rb_node *);
struct rb_node *rb_first(struct rb_root *);

/* Fast replacement of a single node without remove/rebalance/add/rebalance */
extern void rb_replace_node(struct rb_node *victim, struct rb_node *new, 
                            struct rb_root *root);
void rb_link_node(struct rb_node * node, struct rb_node * parent,
             struct rb_node ** rb_link);
void rb_insert_color(struct rb_node *, struct rb_root *);
void rb_erase(struct rb_node *, struct rb_root *);;             


#endif  /* DBA_REDIS_DBCACHECONV_TRIETREE_H */
