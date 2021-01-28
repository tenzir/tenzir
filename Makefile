prepare:
	sed -i 's/`/__LQUOTE__/g' `git ls-files -- '*.hpp' '*.cpp' ':!:*logger.?pp'`
	sed -i "s/'/__SINGLEQUOTE__/g" `git ls-files -- '*.hpp' '*.cpp' ':!:*logger.?pp'`
	sed -i "s/#/__HASHTAG__/g" `git ls-files -- '*.hpp' '*.cpp' ':!:*logger.?pp'`

apply:
	git ls-files -- '*.hpp' '*.cpp' ':!:*logger*' | xargs -I '{}' sh -c "m4 -P rewrite_logs.m4 {} | sponge {}"

cleanup:
	sed -i 's/__LQUOTE__/`/g' `git ls-files -- '*.hpp' '*.cpp' ':!:*logger.?pp'`
	sed -i "s/__SINGLEQUOTE__/'/g" `git ls-files -- '*.hpp' '*.cpp' ':!:*logger.?pp'`
	sed -i "s/__COMMA__/,/g" `git ls-files -- '*.hpp' '*.cpp' ':!:*logger.?pp'`
	sed -i "s/__HASHTAG__/#/g" `git ls-files -- '*.hpp' '*.cpp' ':!:*logger.?pp'`
	sed -i 's/{} "/{}"/g' `git ls-files -- '*.hpp' '*.cpp' ':!:*logger.?pp'`
	sed -i 's/" ,  ) ;/");/' `git ls-files -- '*.hpp' '*.cpp' ':!:*logger.?pp'`
	sed -i 's/{}" , ,/{}", /' `git ls-files -- '*.hpp' '*.cpp' ':!:*logger.?pp'`
	sed -i 's/(" {}/("{}/' `git ls-files -- '*.hpp' '*.cpp' ':!:*logger.?pp'`
	sed -i '/./,$$!d' `git ls-files -- '*.hpp' '*.cpp' ':!:*logger.?pp'`
	#git format-branch
