/*
 * Copyright (c) 2021 Huawei Technologies Co., Ltd
 *
 * This software is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package reliability

// TestSetKey: Test Usefulness in set key expireTime
/*func TestSetKey(t *testing.T) {
	store, err := NewRedisStorage(testRedisConfig)
	if err != nil {
		t.Errorf("New test failed")
		return
	}
	cases := []struct {
		key   string
		value string
		time  int
	}{
		{"testKey01", "testValue01", 10},
		{"testKey02", "testValue02", 60},
	}

	for _, c := range cases {
		err := store.Set(c.key, c.value, c.time)
		if err != nil {
			t.Errorf("redis set data failed")
		}
		value, err := store.Get(c.key)
		if value != c.value {
			t.Fatalf("redis get value immediately failed")
		}
		time.Sleep(time.Second * (time.Duration(c.time) + 2))
		value1, err := store.Get(c.key)
		if value1 != "" {
			t.Log("redis set data expireTime failed")
		}
	}
}*/
