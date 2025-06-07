UPDATE tbl__raw__dim_cam_info
SET district = CASE
    -- Quận 1
    WHEN id = '662b811d1afb9c00172dcc1d' THEN 'Quận 1'
    WHEN id = '662b85bf1afb9c00172dd149' THEN 'Quận 1'
    WHEN id = '5deb576d1dc17d7c5515ad14' THEN 'Quận 1'
    WHEN id = '662b81721afb9c00172dcc44' THEN 'Quận 1'
    WHEN id = '662b80721afb9c00172dcb28' THEN 'Quận 1'
    WHEN id = '662b857b1afb9c00172dd106' THEN 'Quận 1'
    WHEN id = '5deb576d1dc17d7c5515ad1e' THEN 'Quận 1'
    WHEN id = '662b7d8a1afb9c00172dc71f' THEN 'Quận 1'
    WHEN id = '5deb576d1dc17d7c5515ad06' THEN 'Quận 1'
    WHEN id = '5deb576d1dc17d7c5515ad0c' THEN 'Quận 1'
    WHEN id = '662b7f251afb9c00172dc8bc' THEN 'Quận 1'
    WHEN id = '662b85481afb9c00172dd0f1' THEN 'Quận 1'
    WHEN id = '662b7f9f1afb9c00172dca50' THEN 'Quận 1'
    WHEN id = '662b85031afb9c00172dd0dc' THEN 'Quận 1'
    WHEN id = '662b862a1afb9c00172dd1ff' THEN 'Quận 1'
    WHEN id = '58af994abd82540010390c37' THEN 'Quận 1'
    WHEN id = '5deb576d1dc17d7c5515ad13' THEN 'Quận 1'
    WHEN id = '662b843d1afb9c00172dd02d' THEN 'Quận 1'
    WHEN id = '662b81a31afb9c00172dcc65' THEN 'Quận 1'
    WHEN id = '662b81eb1afb9c00172dcc85' THEN 'Quận 1'
    WHEN id = '5deb576d1dc17d7c5515ad19' THEN 'Quận 1'
    WHEN id = '662b80b91afb9c00172dcb5b' THEN 'Quận 1'
    WHEN id = '5deb576d1dc17d7c5515ad16' THEN 'Quận 1'
    WHEN id = '662b85f51afb9c00172dd1c2' THEN 'Quận 1'
    WHEN id = '662b84771afb9c00172dd076' THEN 'Quận 1'
    WHEN id = '5deb576d1dc17d7c5515ad03' THEN 'Quận 1'
    WHEN id = '6318283cc9eae60017a19f0c' THEN 'Quận 1'
    WHEN id = '6318287ec9eae60017a19f36' THEN 'Quận 1'
    WHEN id = '649da77ea6068200171a6dd4' THEN 'Quận 1'
    WHEN id = '649da72ca6068200171a6dbb' THEN 'Quận 1'
    WHEN id = '5deb576d1dc17d7c5515ad0b' THEN 'Quận 1'
    -- Quận 4
    WHEN id = '63ae7727bfd3d90017e8f14a' THEN 'Quận 4'
    WHEN id = '5deb576d1dc17d7c5515ad1c' THEN 'Quận 4'
    WHEN id = '63ae77bfbfd3d90017e8f18f' THEN 'Quận 4'
    WHEN id = '63b6617ebfd3d90017eaa50b' THEN 'Quận 4'
    WHEN id = '63b661a3bfd3d90017eaa520' THEN 'Quận 4'
    WHEN id = '63ae777cbfd3d90017e8f177' THEN 'Quận 4'
    WHEN id = '63ae7759bfd3d90017e8f162' THEN 'Quận 4'
    WHEN id = '63ae7893bfd3d90017e8f1e1' THEN 'Quận 4'
    WHEN id = '63ae76ddbfd3d90017e8f11b' THEN 'Quận 4'
    WHEN id = '63ae76afbfd3d90017e8f106' THEN 'Quận 4'
    WHEN id = '63ae768dbfd3d90017e8f0f1' THEN 'Quận 4'
    WHEN id = '63ae7669bfd3d90017e8f0d9' THEN 'Quận 4'
    -- Quận 5
    WHEN id = '662b4d781afb9c00172d8571' THEN 'Quận 5'
    WHEN id = '5deb576d1dc17d7c5515ad22' THEN 'Quận 5'
    WHEN id = '662b4ecb1afb9c00172d8692' THEN 'Quận 5'
    WHEN id = '66b1c311779f740018674083' THEN 'Quận 5'
    WHEN id = '662b4de41afb9c00172d85c5' THEN 'Quận 5'
    WHEN id = '66b1c1f2779f740018673f0d' THEN 'Quận 5'
    WHEN id = '5deb576d1dc17d7c5515ad20' THEN 'Quận 5'
    WHEN id = '5b632a79fd4edb0019c7dc0f' THEN 'Quận 5'
    WHEN id = '63b3c274bfd3d90017e9ab93' THEN 'Quận 5'
    WHEN id = '5822f23aedeb6c0012a2d6a8' THEN 'Quận 5'
    WHEN id = '5b728aafca0577001163ff7e' THEN 'Quận 5'
    WHEN id = '5b632b60fd4edb0019c7dc12' THEN 'Quận 5'
    WHEN id = '5b0b7aba0e517b00119fd800' THEN 'Quận 5'
    WHEN id = '5b0b7bbe0e517b00119fd806' THEN 'Quận 5'
    WHEN id = '5b6005b6fd4edb0019c7db25' THEN 'Quận 5'
    WHEN id = '5b6329fdfd4edb0019c7dc0b' THEN 'Quận 5'
    WHEN id = '5d8cd3b7766c880017188942' THEN 'Quận 5'
    WHEN id = '5d8cd49f766c880017188944' THEN 'Quận 5'
    WHEN id = '5d8cd1f9766c880017188938' THEN 'Quận 5'
    WHEN id = '66b1c1bf779f740018673ef2' THEN 'Quận 5'
    WHEN id = '66b1c158779f740018673eb4' THEN 'Quận 5'
    WHEN id = '662b4e201afb9c00172d85f9' THEN 'Quận 5'
    WHEN id = '662b4e581afb9c00172d862f' THEN 'Quận 5'
    WHEN id = '662b4efc1afb9c00172d86bc' THEN 'Quận 5'
    -- Quận 10
    WHEN id = '5deb576d1dc17d7c5515acf3' THEN 'Quận 10'
    WHEN id = '6623e7526f998a001b252407' THEN 'Quận 10'
    WHEN id = '5deb576d1dc17d7c5515acf5' THEN 'Quận 10'
    WHEN id = '63ae7966bfd3d90017e8f240' THEN 'Quận 10'
    WHEN id = '5deb576d1dc17d7c5515ad23' THEN 'Quận 10'
    WHEN id = '6623e6b86f998a001b2523b8' THEN 'Quận 10'
    WHEN id = '6623e5d66f998a001b25235a' THEN 'Quận 10'
    WHEN id = '66b1c34d779f74001867409e' THEN 'Quận 10'
    WHEN id = '6623e7076f998a001b2523ea' THEN 'Quận 10'
    WHEN id = '631955e7c9eae60017a1c30a' THEN 'Quận 10'
    WHEN id = '63ae7a74bfd3d90017e8f2c7' THEN 'Quận 10'
    WHEN id = '63ae7a50bfd3d90017e8f2b2' THEN 'Quận 10'
    WHEN id = '63ae7be0bfd3d90017e8f3a8' THEN 'Quận 10'
    WHEN id = '63ae7c12bfd3d90017e8f3c0' THEN 'Quận 10'
    WHEN id = '63ae7af4bfd3d90017e8f32c' THEN 'Quận 10'
    WHEN id = '63ae7b3cbfd3d90017e8f34d' THEN 'Quận 10'
    WHEN id = '63ae7a26bfd3d90017e8f29a' THEN 'Quận 10'
    WHEN id = '63ae7a9cbfd3d90017e8f303' THEN 'Quận 10'
    WHEN id = '66b1c370779f7400186740b3' THEN 'Quận 10'
    WHEN id = '66b1c398779f7400186740e3' THEN 'Quận 10'
    -- Quận 3
    WHEN id = '5deb576d1dc17d7c5515ad10' THEN 'Quận 3'
    WHEN id = '5deb576d1dc17d7c5515acfb' THEN 'Quận 3'
    WHEN id = '5deb576d1dc17d7c5515ad17' THEN 'Quận 3'
    WHEN id = '5deb576d1dc17d7c5515ad02' THEN 'Quận 3'
    WHEN id = '6623e3ea6f998a001b2522ae' THEN 'Quận 3'
    WHEN id = '5deb576d1dc17d7c5515acfd' THEN 'Quận 3'
    WHEN id = '5deb576d1dc17d7c5515acf2' THEN 'Quận 3'
    WHEN id = '5deb576d1dc17d7c5515ad04' THEN 'Quận 3'
    WHEN id = '5deb576d1dc17d7c5515acf9' THEN 'Quận 3'
    WHEN id = '6623e31e6f998a001b252250' THEN 'Quận 3'
    WHEN id = '5deb576d1dc17d7c5515acfc' THEN 'Quận 3'
    WHEN id = '662b83381afb9c00172dcf88' THEN 'Quận 3'
    WHEN id = '6623e5776f998a001b252337' THEN 'Quận 3'
    WHEN id = '662b84c11afb9c00172dd0b5' THEN 'Quận 3'
    WHEN id = '5deb576d1dc17d7c5515ad11' THEN 'Quận 3'
    WHEN id = '662b830e1afb9c00172dcf50' THEN 'Quận 3'
    WHEN id = '5deb576d1dc17d7c5515acfe' THEN 'Quận 3'
    WHEN id = '6623e43e6f998a001b2522cb' THEN 'Quận 3'
    WHEN id = '6623e5066f998a001b252317' THEN 'Quận 3'
    WHEN id = '5deb576d1dc17d7c5515ad18' THEN 'Quận 3'
    WHEN id = '6623e3a26f998a001b252291' THEN 'Quận 3'
    WHEN id = '6623e4b06f998a001b2522f1' THEN 'Quận 3'
    WHEN id = '662b7ce71afb9c00172dc676' THEN 'Quận 3'
    WHEN id = '5deb576d1dc17d7c5515ad0f' THEN 'Quận 3'
    WHEN id = '5a823d555058170011f6eaa2' THEN 'Quận 3'
    WHEN id = '5ad0621c98d8fc001102e268' THEN 'Quận 3'
    WHEN id = '5deb576d1dc17d7c5515ad0e' THEN 'Quận 3'
    WHEN id = '63ae73cebfd3d90017e8f00d' THEN 'Quận 3'
    WHEN id = '63ae7a9cbfd3d90017e8f303' THEN 'Quận 3'
    WHEN id = '63ae75debfd3d90017e8f082' THEN 'Quận 3'
    WHEN id = '63ae75f9bfd3d90017e8f097' THEN 'Quận 3'
    WHEN id = '5deb576d1dc17d7c5515ad01' THEN 'Quận 3'
    WHEN id = '5deb576d1dc17d7c5515acf8' THEN 'Quận 3'
    WHEN id = '5deb576d1dc17d7c5515ad15' THEN 'Quận 3'
    WHEN id = '6623df636f998a001b251e92' THEN 'Quận 3'
    WHEN id = '6623e2e16f998a001b252233' THEN 'Quận 3'
    WHEN id = '63195512c9eae60017a1c279' THEN 'Quận 3'
    WHEN id = '63195556c9eae60017a1c2ba' THEN 'Quận 3'
    WHEN id = '63ae75a3bfd3d90017e8f051' THEN 'Quận 3'
    WHEN id = '662b80051afb9c00172dcaf6' THEN 'Quận 3'
    ELSE district
END
WHERE id IN (
    '662b811d1afb9c00172dcc1d', '662b85bf1afb9c00172dd149', '5deb576d1dc17d7c5515ad14', '662b81721afb9c00172dcc44',
    '662b80721afb9c00172dcb28', '662b857b1afb9c00172dd106', '5deb576d1dc17d7c5515ad1e', '662b7d8a1afb9c00172dc71f',
    '5deb576d1dc17d7c5515ad06', '5deb576d1dc17d7c5515ad0c', '662b7f251afb9c00172dc8bc', '662b85481afb9c00172dd0f1',
    '662b7f9f1afb9c00172dca50', '662b85031afb9c00172dd0dc', '662b862a1afb9c00172dd1ff', '58af994abd82540010390c37',
    '5deb576d1dc17d7c5515ad13', '662b843d1afb9c00172dd02d', '662b81a31afb9c00172dcc65', '662b81eb1afb9c00172dcc85',
    '5deb576d1dc17d7c5515ad19', '662b80b91afb9c00172dcb5b', '5deb576d1dc17d7c5515ad16', '662b85f51afb9c00172dd1c2',
    '662b84771afb9c00172dd076', '5deb576d1dc17d7c5515ad03', '6318283cc9eae60017a19f0c', '6318287ec9eae60017a19f36',
    '649da77ea6068200171a6dd4', '649da72ca6068200171a6dbb', '5deb576d1dc17d7c5515ad0b', '63ae7727bfd3d90017e8f14a',
    '5deb576d1dc17d7c5515ad1c', '63ae77bfbfd3d90017e8f18f', '63b6617ebfd3d90017eaa50b', '63b661a3bfd3d90017eaa520',
    '63ae777cbfd3d90017e8f177', '63ae7759bfd3d90017e8f162', '63ae7893bfd3d90017e8f1e1', '63ae76ddbfd3d90017e8f11b',
    '63ae76afbfd3d90017e8f106', '63ae768dbfd3d90017e8f0f1', '63ae7669bfd3d90017e8f0d9', '662b4d781afb9c00172d8571',
    '5deb576d1dc17d7c5515ad22', '662b4ecb1afb9c00172d8692', '66b1c311779f740018674083', '662b4de41afb9c00172d85c5',
    '66b1c1f2779f740018673f0d', '5deb576d1dc17d7c5515ad20', '5b632a79fd4edb0019c7dc0f', '63b3c274bfd3d90017e9ab93',
    '5822f23aedeb6c0012a2d6a8', '5b728aafca0577001163ff7e', '5b632b60fd4edb0019c7dc12', '5b0b7aba0e517b00119fd800',
    '5b0b7bbe0e517b00119fd806', '5b6005b6fd4edb0019c7db25', '5b6329fdfd4edb0019c7dc0b', '5d8cd3b7766c880017188942',
    '5d8cd49f766c880017188944', '5d8cd1f9766c880017188938', '66b1c1bf779f740018673ef2', '66b1c158779f740018673eb4',
    '662b4e201afb9c00172d85f9', '662b4e581afb9c00172d862f', '662b4efc1afb9c00172d86bc', '5deb576d1dc17d7c5515acf3',
    '6623e7526f998a001b252407', '5deb576d1dc17d7c5515acf5', '63ae7966bfd3d90017e8f240', '5deb576d1dc17d7c5515ad23',
    '6623e6b86f998a001b2523b8', '6623e5d66f998a001b25235a', '66b1c34d779f74001867409e', '6623e7076f998a001b2523ea',
    '631955e7c9eae60017a1c30a', '63ae7a74bfd3d90017e8f2c7', '63ae7a50bfd3d90017e8f2b2', '63ae7be0bfd3d90017e8f3a8',
    '63ae7c12bfd3d90017e8f3c0', '63ae7af4bfd3d90017e8f32c', '63ae7b3cbfd3d90017e8f34d', '63ae7a26bfd3d90017e8f29a',
    '63ae7a9cbfd3d90017e8f303', '66b1c370779f7400186740b3', '66b1c398779f7400186740e3', '5deb576d1dc17d7c5515ad10',
    '5deb576d1dc17d7c5515acfb', '5deb576d1dc17d7c5515ad17', '5deb576d1dc17d7c5515ad02', '6623e3ea6f998a001b2522ae',
    '5deb576d1dc17d7c5515acfd', '5deb576d1dc17d7c5515acf2', '5deb576d1dc17d7c5515ad04', '5deb576d1dc17d7c5515acf9',
    '6623e31e6f998a001b252250', '5deb576d1dc17d7c5515acfc', '662b83381afb9c00172dcf88', '6623e5776f998a001b252337',
    '662b84c11afb9c00172dd0b5', '5deb576d1dc17d7c5515ad11', '662b830e1afb9c00172dcf50', '5deb576d1dc17d7c5515acfe',
    '6623e43e6f998a001b2522cb', '6623e5066f998a001b252317', '5deb576d1dc17d7c5515ad18', '6623e3a26f998a001b252291',
    '6623e4b06f998a001b2522f1', '662b7ce71afb9c00172dc676', '5deb576d1dc17d7c5515ad0f', '5a823d555058170011f6eaa2',
    '5ad0621c98d8fc001102e268', '5deb576d1dc17d7c5515ad0e', '63ae73cebfd3d90017e8f00d', '63ae7a9cbfd3d90017e8f303',
    '63ae75debfd3d90017e8f082', '63ae75f9bfd3d90017e8f097', '5deb576d1dc17d7c5515ad01', '5deb576d1dc17d7c5515acf8',
    '5deb576d1dc17d7c5515ad15', '6623df636f998a001b251e92', '6623e2e16f998a001b252233', '63195512c9eae60017a1c279',
    '63195556c9eae60017a1c2ba', '63ae75a3bfd3d90017e8f051', '662b80051afb9c00172dcaf6'
);
