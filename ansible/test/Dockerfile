FROM fedora

RUN dnf -y install openssh-server sudo; dnf clean all; systemctl enable sshd

# Setup the default user.
RUN useradd -rm -d /home/deployer -s /bin/bash -g root -G wheel deployer
RUN echo 'deployer:foobar' | chpasswd
RUN sed -i '/^%wheel/d; s/^# %wheel/%wheel/' /etc/sudoers

EXPOSE 80

CMD [ "/sbin/init" ]
