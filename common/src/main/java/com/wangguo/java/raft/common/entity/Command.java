package com.wangguo.java.raft.common.entity;

import lombok.*;

import java.io.Serializable;

@Setter
@Getter
@ToString
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Command implements Serializable {
    String key;
    String value;
}
